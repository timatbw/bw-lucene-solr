package org.apache.solr.search.facet;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;

public class TermFrequencyCounter {
  private final Map<String, Integer> counts;
  private boolean overflow;

  public TermFrequencyCounter() {
    this.counts = new HashMap<>();
  }

  public Map<String, Integer> getCounts() {
    return this.counts;
  }

  public void add(String value) {
    counts.merge(value, 1, Integer::sum);
  }

  public SimpleOrderedMap<Object> serialize(int limit) {
    SimpleOrderedMap<Object> result = new SimpleOrderedMap<>();

    if (limit < counts.size()) {
      result.add("counts", getTopCounts(counts, limit));
      result.add("overflow", Boolean.TRUE);
    } else {
      result.add("counts", counts);
      result.add("overflow", Boolean.FALSE);
    }

    return result;
  }

  private Map<String, Integer> getTopCounts(Map<String, Integer> counters, int limit) {
    return counters.entrySet()
      .stream()
      .sorted((l, r) -> r.getValue() - l.getValue()) // sort by value descending
      .limit(limit)
      .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  public TermFrequencyCounter merge(NamedList<Object> serialized) {
    final Map<String, Integer> counts = (Map<String, Integer>) serialized.get("counts");
    if (counts != null) {
      counts.forEach((value, freq) -> this.counts.merge(value, freq, Integer::sum));
    }

    final Boolean overflow = (Boolean) serialized.get("overflow");
    if (overflow != null) {
      this.overflow = this.overflow || overflow;
    }

    return this;
  }

  public SimpleOrderedMap<Object> toFrequencyOfFrequencies() {
    SimpleOrderedMap<Object> result = new SimpleOrderedMap<>();

    Map<Integer, Integer> frequencies = new LinkedHashMap<>();
    counts.forEach((value, freq) -> frequencies.merge(freq, 1, Integer::sum));

    result.add("frequencies", frequencies);
    result.add("overflow", overflow);

    return result;
  }
}
