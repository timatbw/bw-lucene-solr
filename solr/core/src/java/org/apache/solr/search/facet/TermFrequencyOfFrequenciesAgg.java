package org.apache.solr.search.facet;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.lucene.queries.function.ValueSource;
import org.apache.solr.search.FunctionQParser;
import org.apache.solr.search.SyntaxError;
import org.apache.solr.search.ValueSourceParser;

public class TermFrequencyOfFrequenciesAgg extends SimpleAggValueSource {
  private final int termLimit;

  public TermFrequencyOfFrequenciesAgg(ValueSource vs, int termLimit) {
    super("termfreqfreq", vs);

    this.termLimit = termLimit;
  }

  @Override
  public SlotAcc createSlotAcc(FacetContext fcontext, int numDocs, int numSlots) {
    return new TermFrequencySlotAcc(getArg(), fcontext, numSlots, termLimit);
  }

  @Override
  public FacetMerger createFacetMerger(Object prototype) {
    return new Merger();
  }

  public static class Parser extends ValueSourceParser {
    @Override
    public ValueSource parse(FunctionQParser fp) throws SyntaxError {
      ValueSource vs = fp.parseValueSource();

      int termLimit = Integer.MAX_VALUE;
      if (fp.hasMoreArguments()) {
        termLimit = fp.parseInt();
      }

      return new TermFrequencyOfFrequenciesAgg(vs, termLimit);
    }
  }

  private static class Merger extends FacetMerger {
    private final TermFrequencyCounter result;

    public Merger() {
      this.result = new TermFrequencyCounter();
    }

    @Override
    public void merge(Object facetResult, Context mcontext) {
      if (facetResult instanceof Map) {
        result.merge((Map<String, Integer>) facetResult);
      }
    }

    @Override
    public void finish(Context mcontext) {
      // never called
    }

    @Override
    public Object getMergedResult() {
      Map<Integer, Integer> map = new LinkedHashMap<>();

      result.getCounters()
        .forEach((value, freq) -> map.merge(freq, 1, Integer::sum));

      return map;
    }
  }
}
