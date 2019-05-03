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
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.IntFunction;
import java.util.stream.Collectors;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.solr.search.DocSet;

public abstract class CombiningAggValueSource extends AggValueSource {

  private final List<AggValueSource> subAggs;

  public CombiningAggValueSource(String name, List<AggValueSource> subAggs) {
    super(name);
    this.subAggs = subAggs;
  }

  @Override
  public FacetMerger createFacetMerger(Object prototype) {
    List<Object> prototypeResult = (List<Object>)prototype;
    if (prototypeResult.size() != subAggs.size()) {
      throw new IllegalStateException("Aggregate merge prototype has unexpected size : " + prototypeResult.size());
    }
    List<FacetMerger> subMergers = new ArrayList<>(subAggs.size());
    for (int pos = 0; pos < subAggs.size(); pos++) {
      subMergers.add(subAggs.get(pos).createFacetMerger(prototypeResult.get(pos)));
    }

    return new FacetDoubleMerger() {
      @Override
      public void merge(Object facetResult, Context mcontext) {
        List<Object> shardResult = (List<Object>)facetResult;
        if (shardResult.size() != subMergers.size()) {
          throw new IllegalStateException("Aggregate merge response has unexpected size : " + shardResult.size());
        }
        for (int pos = 0; pos < shardResult.size(); pos++) {
          subMergers.get(pos).merge(shardResult.get(pos), mcontext);
        }
      }

      @Override
      protected double getDouble() {
        List<Number> subValues = new ArrayList<>();
        for (FacetMerger subMerger : subMergers) {
          subValues.add((Number)subMerger.getMergedResult());
        }
        return combine(subValues);
      }
    };
  }

  protected abstract Double combine(List<Number> values);

  @Override
  public SlotAcc createSlotAcc(FacetContext fcontext, int numDocs, int numSlots) throws IOException {
    List<SlotAcc> subAcc = new ArrayList<>(subAggs.size());
    for (AggValueSource sub : subAggs) {
      subAcc.add(sub.createSlotAcc(fcontext, numDocs, numSlots));
    }

    return new SlotAcc(fcontext) {

      // cache of combined values, since compare operations may ask for these repeatedly
      Double[] combinedValues = new Double[numSlots];

      @Override
      public int compare(int slotA, int slotB) {
        try {
          Double valueA = getCombinedValue(slotA);
          Double valueB = getCombinedValue(slotB);
          return Double.compare(valueA, valueB);
        } catch (IOException ioe) {
          throw new RuntimeException("Exception in aggregate comparison", ioe);
        }
      }

      @Override
      public Object getValue(int slotNum) throws IOException {
        if (fcontext.isShard()) {
          List<Object> lst = new ArrayList<>(subAcc.size());
          for (SlotAcc acc : subAcc) {
            Object accValue = acc.getValue(slotNum);
            lst.add(accValue);
          }
          return lst;
        } else {
          return getCombinedValue(slotNum);
        }
      }

      @Override
      public Object getSortableValue(int slotNum) throws IOException {
        return getCombinedValue(slotNum);
      }

      private Double getCombinedValue(int slot) throws IOException {
        if (combinedValues[slot] == null) {
          List<Number> subValues = new ArrayList<>();
          for (SlotAcc acc : subAcc) {
            subValues.add((Number)acc.getSortableValue(slot));
          }
          combinedValues[slot] = combine(subValues);
        }
        return combinedValues[slot];
      }

      @Override
      public void setNextReader(LeafReaderContext readerContext) throws IOException {
        super.setNextReader(readerContext);
        for (SlotAcc acc : subAcc) {
          acc.setNextReader(readerContext);
        }
      }

      @Override
      public int collect(DocSet docs, int slot, IntFunction<SlotContext> slotContext) throws IOException {
        for (SlotAcc acc : subAcc) {
          acc.collect(docs, slot, slotContext);
        }
        return docs.size();
      }

      @Override
      protected void resetIterators() throws IOException {
        for (SlotAcc acc : subAcc) {
          acc.resetIterators();
        }
      }

      @Override
      public void collect(int doc, int slot, IntFunction<SlotContext> slotContext) throws IOException {
        for (SlotAcc acc : subAcc) {
          acc.collect(doc, slot, slotContext);
        }
      }

      @Override
      public void reset() throws IOException {
        Arrays.fill(combinedValues, null);
        for (SlotAcc acc : subAcc) {
          acc.reset();
        }
      }

      @Override
      public void resize(Resizer resizer) {
        combinedValues = resizer.resize(combinedValues, null);
        for (SlotAcc acc : subAcc) {
          acc.resize(resizer);
        }
      }
    };
  }

  @Override
  public int hashCode() {
    return Objects.hash(subAggs.toArray());
  }

  @Override
  public String description() {
    String subAggsDescription = subAggs.stream()
        .map(AggValueSource::description)
        .collect(Collectors.joining(","));
    return name() + "(" + subAggsDescription + ")";
  }
}
