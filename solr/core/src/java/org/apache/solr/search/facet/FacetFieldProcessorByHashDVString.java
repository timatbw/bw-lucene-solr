/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.search.facet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.function.IntFunction;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.SimpleCollector;
import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.DocSetUtil;
import org.apache.solr.search.facet.SlotAcc.CountSlotAcc;
import org.apache.solr.search.facet.SlotAcc.SlotContext;

/**
 * Facets DocValues into a HashMap using the BytesRef as key.
 * Limitations:
 * <ul>
 *   <li>Only for string type fields not numerics (use dvhash for those)</li>
 * </ul>
 */
class FacetFieldProcessorByHashDVString extends FacetFieldProcessor {

  static class TermData {
    int count;
    int slotIndex;
  }

  // Using a regular HashMap hence slots get created dynamically as new keys are found from docvalues
  private HashMap<BytesRef, TermData> table;
  private ArrayList<BytesRef> slotList; // position in List is the slot number, value is key for table
  private int capacity; // how many slots we will need for accs, gets resized later if needed
  private boolean multiValuedField;

  FacetFieldProcessorByHashDVString(FacetContext fcontext, FacetField freq, SchemaField sf) {
    super(fcontext, freq, sf);
    if (freq.mincount == 0) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          getClass()+" doesn't support mincount=0");
    }
    if (freq.prefix != null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          getClass()+" doesn't support prefix"); // yet, but it could
    }
    FieldInfo fieldInfo = fcontext.searcher.getSlowAtomicReader().getFieldInfos().fieldInfo(sf.getName());
    if (fieldInfo != null &&
        fieldInfo.getDocValuesType() != DocValuesType.SORTED &&
        fieldInfo.getDocValuesType() != DocValuesType.SORTED_SET) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          getClass()+" only supports string with docValues");
    }
    multiValuedField = sf.multiValued() || sf.getType().multiValuedFieldCache();
  }

  @Override
  public void process() throws IOException {
    super.process();
    response = calcFacets();
    table = null;//gc
    slotList = null;
  }

  private SimpleOrderedMap<Object> calcFacets() throws IOException {

    int possibleValues = fcontext.base.size();
    int hashSize = BitUtil.nextHighestPowerOfTwo((int) (possibleValues * (1 / 0.7) + 1));
    hashSize = Math.min(hashSize, 1024);

    table = new HashMap<>(hashSize);
    slotList = new ArrayList<>();

    // The initial value of capacity. Note that slot capacity and resizing only does anything
    // if you're using allBuckets:true or sorting by a stat, otherwise it's a no-op.
    capacity = Math.max(128, possibleValues / 10);

    createCollectAcc();

    collectDocs();

    return super.findTopSlots(table.size(), table.size(),
        slotNum -> slotList.get(slotNum).utf8ToString(), // getBucketValFromSlotNum
        val -> val.toString()); // getFieldQueryVal
  }

  private void createCollectAcc() throws IOException {

    // This only gets used for sorting so doesn't need to collect, just implements compare
    indexOrderAcc = new SlotAcc(fcontext) {
      @Override
      public void collect(int doc, int slot, IntFunction<SlotContext> slotContext) throws IOException {
      }

      @Override
      public int compare(int slotA, int slotB) {
        return slotList.get(slotA).compareTo(slotList.get(slotB));
      }

      @Override
      public Object getValue(int slotNum) throws IOException {
        return null;
      }

      @Override
      public void reset() {
      }

      @Override
      public void resize(Resizer resizer) {
      }
    };

    // This implementation never needs to collect docs, it only gets used to report count per-slot
    // for the response, and if used for sorting by count, both of which it can do by using the table
    countAcc = new CountSlotAcc(fcontext) {
      @Override
      public void incrementCount(int slot, int count) {
        throw new UnsupportedOperationException();
      }

      @Override
      public int getCount(int slot) {
        return table.get(slotList.get(slot)).count;
      }

      @Override
      public Object getValue(int slotNum) {
        return getCount(slotNum);
      }

      @Override
      public void reset() {
        throw new UnsupportedOperationException();
      }

      @Override
      public void collect(int doc, int slot, IntFunction<SlotContext> slotContext) throws IOException {
        throw new UnsupportedOperationException();
      }

      @Override
      public int compare(int slotA, int slotB) {
        return Integer.compare( getCount(slotA), getCount(slotB) );
      }

      @Override
      public void resize(Resizer resizer) {
        throw new UnsupportedOperationException();
      }
    };

    // we set the countAcc & indexAcc first so generic ones won't be created for us.
    // adding 1 extra slot for allBuckets, which always goes on the end
    super.createCollectAcc(fcontext.base.size(), capacity + 1);

    if (freq.allBuckets) {
      allBucketsAcc = new SpecialSlotAcc(fcontext, collectAcc, capacity, otherAccs, 0);
    }
  }

  private void collectDocs() throws IOException {

    if (multiValuedField) {
      DocSetUtil.collectSortedDocSet(fcontext.base, fcontext.searcher.getIndexReader(), new SimpleCollector() {
        SortedSetDocValues values = null;
        HashMap<Long, BytesRef> segOrdinalValueCache; // avoid repeated lookups of the same ordinal, in this seg

        @Override public ScoreMode scoreMode() { return ScoreMode.COMPLETE_NO_SCORES; }

        @Override
        protected void doSetNextReader(LeafReaderContext ctx) throws IOException {
          setNextReaderFirstPhase(ctx);
          values = DocValues.getSortedSet(ctx.reader(), sf.getName());
          segOrdinalValueCache = new HashMap<>((int)values.getValueCount());
        }

        @Override
        public void collect(int segDoc) throws IOException {
          if (values.advanceExact(segDoc)) {
            // TODO not fully clear if values.nextOrd may return duplicates or not (if a doc has the same value twice)
            long previousOrdinal = -1L;
            long ordinal;
            while ((ordinal = values.nextOrd()) != SortedSetDocValues.NO_MORE_ORDS) {
              if (ordinal != previousOrdinal) {
                BytesRef docValue = segOrdinalValueCache.get(ordinal);
                if (docValue == null) {
                  docValue = BytesRef.deepCopyOf(values.lookupOrd(ordinal));
                  segOrdinalValueCache.put(ordinal, docValue);
                }
                collectValFirstPhase(segDoc, docValue);
              }
              previousOrdinal = ordinal;
            }
          }
        }
      });

    } else {
      DocSetUtil.collectSortedDocSet(fcontext.base, fcontext.searcher.getIndexReader(), new SimpleCollector() {
        SortedDocValues values = null;
        HashMap<Integer, BytesRef> segOrdinalValueCache; // avoid repeated lookups of the same ordinal, in this seg

        @Override public ScoreMode scoreMode() { return ScoreMode.COMPLETE_NO_SCORES; }

        @Override
        protected void doSetNextReader(LeafReaderContext ctx) throws IOException {
          setNextReaderFirstPhase(ctx);
          values = DocValues.getSorted(ctx.reader(), sf.getName());
          segOrdinalValueCache = new HashMap<>(values.getValueCount());
        }

        @Override
        public void collect(int segDoc) throws IOException {
          if (values.advanceExact(segDoc)) {
            int docOrdinal = values.ordValue();
            BytesRef docValue = segOrdinalValueCache.get(docOrdinal);
            if (docValue == null) {
              docValue = BytesRef.deepCopyOf(values.binaryValue());
              segOrdinalValueCache.put(docOrdinal, docValue);
            }
            collectValFirstPhase(segDoc, docValue);
          }
        }
      });
    }

  }

  private void collectValFirstPhase(int segDoc, BytesRef val) throws IOException {
    TermData termData = table.get(val);
    if (termData == null) {
      termData = new TermData();
      termData.slotIndex = slotList.size(); // next position in the list
      table.put(val, termData);
      slotList.add(val);
      if (termData.slotIndex >= capacity) {
        resizeAccumulators();
      }
    }
    termData.count++;

    super.collectFirstPhase(segDoc, termData.slotIndex, slotNum -> {
        return new SlotContext(sf.getType().getFieldQuery(null, sf, val.utf8ToString()));
      });
  }

  private void resizeAccumulators() {
    // Our countAcc does not need resizing as it's backed by the table

    if (collectAcc == null && allBucketsAcc == null) {
      return;
    }

    final int oldAllBucketsSlot = capacity;
    capacity *= 2;

    SlotAcc.Resizer resizer = new SlotAcc.Resizer() {
      @Override
      public int getNewSize() {
        return capacity + 1; // extra slot for allBuckets
      }

      @Override
      public int getNewSlot(int oldSlot) {
        if (oldSlot == oldAllBucketsSlot) {
          return capacity;
        }
        return oldSlot;
      }
    };

    if (collectAcc != null) {
      collectAcc.resize(resizer);
    }
    if (allBucketsAcc != null) {
      allBucketsAcc.resize(resizer);
    }
  }
}
