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
import java.util.Comparator;
import java.util.HashMap;
import java.util.function.IntFunction;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.SimpleCollector;
import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.DocSetUtil;
import org.apache.solr.search.facet.SlotAcc.SlotContext;

/**
 * Facets DocValues into a HashMap using the BytesRef as key.
 * Limitations:
 * <ul>
 *   <li>Only for string type fields not numerics (use dvhash for those)</li>
 * </ul>
 */
class FacetFieldProcessorByHashDVString extends FacetFieldProcessor {

  static Comparator<Comparable> COMPARATOR = Comparator.<Comparable>nullsLast(Comparator.naturalOrder());

  static class TermData {
    int count;
    int slotIndex;
    SlotAcc accumulator; // only used if sorting by a stat
  }

  // Using a regular HashMap hence slots get created dynamically as new keys are found from docvalues
  // Note that unlike dvhash we do not using multiple slot accumulators and call resize, instead this
  // approach creates a single-slot accumulator for each table entry
  private HashMap<BytesRef, TermData> table;
  private ArrayList<BytesRef> slotList; // position in List is the slot number, value is key for table
  private AggValueSource sortAgg; // non-null if sorting by a stat

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
  }

  @Override
  public void process() throws IOException {
    super.process();
    response = calcFacets();
    table = null;//gc
    slotList = null;
  }

  private SimpleOrderedMap<Object> calcFacets() throws IOException {


    // TODO: Use the number of indexed terms, if present, as an estimate!
    //    Even for NumericDocValues, we could check for a terms index for an estimate.
    //    Our estimation should aim high to avoid expensive rehashes.

    int possibleValues = fcontext.base.size();
    int hashSize = BitUtil.nextHighestPowerOfTwo((int) (possibleValues * (1 / 0.7) + 1));
    hashSize = Math.min(hashSize, 1024);
    //System.out.println("TPO base size is " + possibleValues + " ... chosen hashmap size is " + hashSize);

    table = new HashMap<>(hashSize);
    slotList = new ArrayList<>();

    // note: these methods/phases align with FacetFieldProcessorByArray's

    createCollectAcc();

    collectDocs();

    return super.findTopSlots(table.size(), table.size(),
        slotNum -> slotList.get(slotNum).utf8ToString(), // getBucketValFromSlotNum
        val -> val.toString()); // getFieldQueryVal
    // TODO confirm the above is OK
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
    super.createCollectAcc(fcontext.base.size(), 1);

    // when sorting by a stat, we create an adaptor SlotAcc that uses the table as its backing data
    if (!"count".equals(freq.sort.sortVariable) && !"index".equals(freq.sort.sortVariable)) {
      sortAgg = freq.getFacetStats().get(freq.sort.sortVariable);
      //System.out.println("TPOx sortAgg=" + sortAgg + " var=" + freq.sort.sortVariable + " and stats has " + freq.getFacetStats().keySet());
      if (sortAgg != null) {
        // Easiest to create a SlotAcc for allBuckets whether we use it or not
        final SlotAcc allBucketsDelegate = sortAgg.createSlotAcc(fcontext, -1, 1);
        collectAcc = new SlotAcc(fcontext) {
          @Override
          public void collect(int doc, int slot, IntFunction<SlotContext> slotContext) throws IOException {
            System.out.println("TPOx called collectAcc with doc " + doc + " for slot " + slot);
            if (slot >= 0) {
              table.get(slotList.get(slot)).accumulator.collect(doc, 0, slotContext);
            } else {
              allBucketsDelegate.collect(doc, 0, slotContext);
            }
          }

          @Override
          public int compare(int slotA, int slotB) {
            //System.out.println("TPO called compare for slots " + slotA + " and " + slotB);
            try {
              int savedFlags = fcontext.flags; // TODO better way to get sortable value from acc
              fcontext.flags &= ~FacetContext.IS_SHARD;
              final Comparable valueA = (Comparable)getValue(slotA);
              final Comparable valueB = (Comparable)getValue(slotB);
              fcontext.flags = savedFlags;
              return COMPARATOR.compare(valueA, valueB);
            } catch (IOException ioe) {
              throw new RuntimeException("Failure during facet slot sort", ioe);
            }
          }

          @Override
          public Object getValue(int slotNum) throws IOException {
            //System.out.println("TPO called getValue for slot " + slotNum);
            if (slotNum >= 0) {
              return table.get(slotList.get(slotNum)).accumulator.getValue(0);
            } else {
              return allBucketsDelegate.getValue(0);
            }
          }

          @Override
          public void setNextReader(LeafReaderContext readerContext) throws IOException {
            //System.out.println("TPO called setNextReader");
            super.setNextReader(readerContext);
            allBucketsDelegate.setNextReader(readerContext);
            for (TermData td : table.values()) {
              td.accumulator.setNextReader(readerContext);
            }
          }

          @Override
          public void reset() throws IOException {
            allBucketsDelegate.reset();
            for (TermData td : table.values()) {
              td.accumulator.reset();
            }
          }

          @Override
          public void resize(Resizer resizer) {
          }
        };
        collectAcc.key = freq.sort.sortVariable;
      }
      sortAcc = collectAcc;
      deferredAggs.remove(freq.sort.sortVariable);
    }

    if (freq.allBuckets) {
      allBucketsAcc = new SpecialSlotAcc(fcontext, collectAcc, -1, otherAccs, 0);
    }
  }

  private void collectDocs() throws IOException {

    if (sf.multiValued()) {
      DocSetUtil.collectSortedDocSet(fcontext.base, fcontext.searcher.getIndexReader(), new SimpleCollector() {
        SortedSetDocValues values = null;
        HashMap<Long, BytesRef> segOrdinalValueCache; // avoid repeated lookups of the same ordinal, in this seg

        @Override public boolean needsScores() { return false; }

        @Override
        protected void doSetNextReader(LeafReaderContext ctx) throws IOException {
          setNextReaderFirstPhase(ctx);
          values = DocValues.getSortedSet(ctx.reader(), sf.getName());
          segOrdinalValueCache = new HashMap<>((int)values.getValueCount());
          System.out.println("TPOm setNextReader to " + ctx.ord + " with base=" + ctx.docBase + " and dv has " +
              values.getValueCount());
        }

        @Override
        public void collect(int segDoc) throws IOException {
          if (values.advanceExact(segDoc)) {
            System.out.println(" TPOm collecting segDoc " + segDoc);
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
                System.out.println("  TPOm found ordinal " + ordinal + " with value " + docValue.utf8ToString());
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

        @Override public boolean needsScores() { return false; }

        @Override
        protected void doSetNextReader(LeafReaderContext ctx) throws IOException {
          setNextReaderFirstPhase(ctx);
          values = DocValues.getSorted(ctx.reader(), sf.getName());
          segOrdinalValueCache = new HashMap<>(values.getValueCount());
          System.out.println("TPO setNextReader to " + ctx.ord + " with base=" + ctx.docBase + " and dv has " +
              values.getValueCount() + " values");
        }

        @Override
        public void collect(int segDoc) throws IOException {
          if (values.advanceExact(segDoc)) {
            int docOrdinal = values.ordValue();
            BytesRef docValue = segOrdinalValueCache.get(docOrdinal);
            if (docValue == null) {
              docValue = BytesRef.deepCopyOf(values.binaryValue());
              segOrdinalValueCache.put(docOrdinal, docValue);
//            } else {
//              System.out.println("  reused cached value of ordinal " + docOrdinal);
            }
            System.out.println("  TPO collecting segDoc " + segDoc + " with ord " + docOrdinal + " with value " + docValue.utf8ToString());
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
      if (sortAgg != null) {
        termData.accumulator = sortAgg.createSlotAcc(fcontext, -1, 1);
        termData.accumulator.setNextReader(sortAcc.currentReaderContext);
      }
      System.out.println("First appearance of " + val.utf8ToString());
      table.put(val, termData);
      slotList.add(val);
    }
    termData.count++;

    super.collectFirstPhase(segDoc, termData.slotIndex, slotNum -> {
        return new SlotContext(sf.getType().getFieldQuery(null, sf, val.utf8ToString()));
      });
  }
}
