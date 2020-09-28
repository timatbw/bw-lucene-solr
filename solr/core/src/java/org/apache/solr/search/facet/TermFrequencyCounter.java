package org.apache.solr.search.facet;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class TermFrequencyCounter {
  private final Map<String, Integer> counters;

  public TermFrequencyCounter() {
    this.counters = new HashMap<>();
  }

  public Map<String, Integer> getCounters() {
    return this.counters;
  }

  public void add(String value) {
    counters.merge(value, 1, Integer::sum);
  }

  public Map<String, Integer> serialize(int limit) {
    if (limit < counters.size()) {
      return counters.entrySet()
        .stream()
        .sorted((l, r) -> r.getValue() - l.getValue()) // sort by value descending
        .limit(limit)
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    } else {
      return counters;
    }
  }

  public TermFrequencyCounter merge(Map<String, Integer> serialized) {
    serialized.forEach((value, freq) -> counters.merge(value, freq, Integer::sum));

    return this;
  }

  public Map<Integer, Integer> toFrequencyOfFrequencies() {
    Map<Integer, Integer> map = new LinkedHashMap<>();

    counters.forEach((value, freq) -> map.merge(freq, 1, Integer::sum));

    return map;
  }
}
