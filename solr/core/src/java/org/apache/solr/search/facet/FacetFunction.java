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
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.function.FunctionRangeQuery;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.Query;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.DocSetBuilder;

public class FacetFunction extends FacetRequestSorted {

  ValueSource valueSource;

  public FacetFunction() {
    mincount = 1;
    limit = -1;
  }

  @Override
  public FacetProcessor createFacetProcessor(FacetContext fcontext) {
    return new FacetFunctionProcessor(fcontext, this);
  }

  @Override
  public FacetMerger createFacetMerger(Object prototype) {
    return new FacetFunctionMerger(this);
  }

  @Override
  public Map<String, Object> getFacetDescription() {
    Map<String, Object> descr = new HashMap<>();
    descr.put("function", valueSource.description());
    return descr;
  }
}

class FacetFunctionMerger extends FacetRequestSortedMerger<FacetFunction> {

  public FacetFunctionMerger(FacetFunction freq) {
    super(freq);
  }

  @Override
  public void merge(Object facetResult, Context mcontext) {
    SimpleOrderedMap<Object> facetResultMap = (SimpleOrderedMap)facetResult;
    List<SimpleOrderedMap> bucketList = (List<SimpleOrderedMap>)facetResultMap.get("buckets");
    mergeBucketList(bucketList, mcontext);
  }

  @Override
  public void finish(Context mcontext) {
    // nothing more to do
  }

  @Override
  public Object getMergedResult() {
    SimpleOrderedMap<Object> result = new SimpleOrderedMap<>();
    sortBuckets();
    result.add("buckets", getPaginatedBuckets());
    return result;
  }
}

class FacetFunctionProcessor extends FacetProcessor<FacetFunction> {

  protected boolean firstPhase;
  protected int segBase = 0;
  protected FunctionValues functionValues;
  protected Map<Object, Bucket> buckets = new HashMap<>();
  protected Comparator<Bucket> comparator;
  protected boolean isSortingByStat;

  FacetFunctionProcessor(FacetContext fcontext, FacetFunction freq) {
    super(fcontext, freq);
    chooseComparator();
  }

  @Override
  public void process() throws IOException {
    super.process();

    firstPhase = true;
    collect(fcontext.base, 0);

    firstPhase = false;
    ArrayList<Bucket> bucketList = new ArrayList<>();
    for (Bucket bucket : buckets.values()) {
      if (!fcontext.isShard() && bucket.getCount() < freq.mincount) {
        continue;
      }
      Object key = bucket.getKey();
      bucket.response = new SimpleOrderedMap<>();
      bucket.response.add("val", key);

      // We're passing the computed DocSet for the bucket, but we'll also provide a Query to recreate it, although
      // that should only be needed by the excludeTags logic
      Query bucketFilter = new FunctionRangeQuery(freq.valueSource, key.toString(), key.toString(), true, true);
      countAcc = null;
      fillBucket(bucket.response, bucketFilter, bucket.getDocSet(), (fcontext.flags & FacetContext.SKIP_FACET)!=0, fcontext.facetInfo);
      if (isSortingByStat) {
        bucket.sortValue = accMap.get(freq.sortVariable).getValue(0);
      }
      bucketList.add(bucket);
    }

    List<SimpleOrderedMap<Object>> responseBuckets = bucketList.stream()
        .sorted(comparator)
        .skip(fcontext.isShard() ? 0 : freq.offset)
        // TODO refactor calculation of effectiveLimit using overrequest and use here
        .limit(fcontext.isShard() || freq.limit < 0 ? Integer.MAX_VALUE : freq.limit)
        .map(bucket -> bucket.response)
        .collect(Collectors.toList());

    response = new SimpleOrderedMap<>();
    response.add("buckets", responseBuckets);
  }

  private void chooseComparator() {
    if ("count".equals(freq.sortVariable) || fcontext.isShard()) {
      comparator = Bucket.COUNT_COMPARATOR.thenComparing(Bucket.KEY_COMPARATOR);
    } else if ("index".equals(freq.sortVariable)) {
      comparator = Bucket.KEY_COMPARATOR;
    } else if (freq.getFacetStats().containsKey(freq.sortVariable)) {
      comparator = Bucket.SORTSTAT_COMPARATOR.thenComparing(Bucket.KEY_COMPARATOR);
      isSortingByStat = true;
    } else {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          "Unknown facet sort value " + freq.sortVariable);
    }
    if (FacetRequest.SortDirection.desc.equals(freq.sortDirection)) {
      comparator = comparator.reversed();
    }
  }

  @Override
  void collect(int segDoc, int slot) throws IOException {
    if (firstPhase) {
      // Sift the documents into buckets, 1 bucket per distinct result value from the function
      Object objectVal = functionValues.objectVal(segDoc);
      if (objectVal == null) {
        return; // TODO consider a 'missing' bucket for these?
      }
      Bucket bucket = buckets.computeIfAbsent(objectVal, key -> new Bucket(key, fcontext.searcher.maxDoc(), fcontext.base.size()));
      bucket.addDoc(segDoc + segBase);
    } else {
      super.collect(segDoc, slot);
    }
  }

  @Override
  void setNextReader(LeafReaderContext readerContext) throws IOException {
    if (firstPhase) {
      segBase = readerContext.docBase;
      functionValues = freq.valueSource.getValues(fcontext.qcontext, readerContext);
    } else {
      super.setNextReader(readerContext);
    }
  }

  static class Bucket {

    static final Comparator<Bucket> KEY_COMPARATOR = (b1, b2) -> ((Comparable)b1.getKey()).compareTo(b2.getKey());
    static final Comparator<Bucket> COUNT_COMPARATOR = (b1, b2) -> Integer.compare(b1.getCount(), b2.getCount());
    static final Comparator<Bucket> SORTSTAT_COMPARATOR = (b1, b2) -> {
      if (b1.sortValue == null || b2.sortValue == null) {
        return 0;
      } else {
        return ((Comparable)b1.sortValue).compareTo(b2.sortValue);
      }
    };

    private final Object key;
    private final DocSetBuilder docSetBuilder;
    private DocSet docSet;
    Object sortValue;
    SimpleOrderedMap<Object> response;

    // maxDoc and parentSize help decide what kind of DocSet to use.
    // parentSize is an upper bound on how many docs will be added to this bucket
    Bucket(Object key, int maxDoc, int parentSize) {
      this.key = key;

      // Crossover point where bitset more space-efficient than sorted ints is maxDoc >> 5
      // i.e. 32 bit int vs 1 bit
      // builder uses >>> 7 on maxDoc as its threshold, hence we'll use >> 2 on our
      // expected upper bound of doc set size
      this.docSetBuilder = new DocSetBuilder(maxDoc, parentSize >> 2);
    }

    Object getKey() {
      return key;
    }

    int getCount() {
      return getDocSet().size();
    }

    void addDoc(int doc) {
      docSetBuilder.add(doc);
    }

    DocSet getDocSet() {
      if (docSet == null) {
        docSet = docSetBuilder.buildUniqueInOrder(null);
      }
      return docSet;
    }
  }
}
