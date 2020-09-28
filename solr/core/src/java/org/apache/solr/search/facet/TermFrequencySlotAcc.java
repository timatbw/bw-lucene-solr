package org.apache.solr.search.facet;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.function.IntFunction;

import org.apache.lucene.queries.function.ValueSource;

public class TermFrequencySlotAcc extends FuncSlotAcc {
  private TermFrequencyCounter[] result;
  private final int termLimit;

  public TermFrequencySlotAcc(ValueSource values, FacetContext fcontext, int numSlots, int termLimit) {
    super(values, fcontext, numSlots);

    this.result = new TermFrequencyCounter[numSlots];
    this.termLimit = termLimit;
  }

  @Override
  public void collect(int doc, int slot, IntFunction<SlotContext> slotContext) throws IOException {
    if (result[slot] == null) {
      result[slot] = new TermFrequencyCounter();
    }
    result[slot].add(values.strVal(doc));
  }

  @Override
  public int compare(int slotA, int slotB) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Object getValue(int slotNum) {
    if (fcontext.isShard()) {
      if (result[slotNum] != null) {
        return result[slotNum].serialize(termLimit);
      } else {
        return Collections.emptyList();
      }
    } else {
      if (result[slotNum] != null) {
        return result[slotNum].toFrequencyOfFrequencies();
      } else {
        return Collections.emptyMap();
      }
    }
  }

  @Override
  public void reset() {
    Arrays.fill(result, null);
  }

  @Override
  public void resize(Resizer resizer) {
    result = resizer.resize(result, null);
  }
}
