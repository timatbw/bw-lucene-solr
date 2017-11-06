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
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.function.FunctionRangeQuery;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.FixedBitSet;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.search.BitDocSet;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.HashDocSet;

public class FacetFunction extends FacetRequestSorted {

  ValueSource valueSource;

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
    // TODO re-sorting and pagination
    SimpleOrderedMap<Object> result = new SimpleOrderedMap<>();
    result.add("buckets", buckets.values().stream()
        .filter(b -> b.getCount() >= freq.mincount)
        .map(FacetBucket::getMergedBucket)
        .collect(Collectors.toList()));
    return result;
  }
}

class FacetFunctionProcessor extends FacetProcessor<FacetFunction> {

  protected boolean firstPhase;
  protected int segBase = 0;
  protected FunctionValues functionValues;
  protected Map<Object, Bucket> buckets = new HashMap<>();

  FacetFunctionProcessor(FacetContext fcontext, FacetFunction freq) {
    super(fcontext, freq);
  }

  @Override
  public void process() throws IOException {
    super.process();

    firstPhase = true;
    collect(fcontext.base, 0);
    // TODO sorting and pagination and mincount filtering here

    firstPhase = false;
    List<SimpleOrderedMap<Object>> bucketList = new ArrayList<>();
    for (Object key : buckets.keySet()) {
      Bucket bucket = buckets.get(key);
      if (!fcontext.isShard() && bucket.getDocSet().size() < freq.mincount) {
        continue;
      }
      SimpleOrderedMap<Object> bucketResponse = new SimpleOrderedMap<>();
      bucketResponse.add("val", key);

      // We're passing the computed DocSet for the bucket, but we'll also provide a Query to recreate it, although
      // that should only be needed by the excludeTags logic
      Query bucketFilter = new FunctionRangeQuery(freq.valueSource, key.toString(), key.toString(), true, true);
      fillBucket(bucketResponse, bucketFilter, bucket.getDocSet(), (fcontext.flags & FacetContext.SKIP_FACET)!=0, fcontext.facetInfo);

      bucketList.add(bucketResponse);
    }
    response = new SimpleOrderedMap<>();
    response.add("buckets", bucketList);
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
    private DocSet docSet;

    // maxDoc and parentSize help decide what kind of DocSet to use.
    // parentSize is an upper bound on how many docs will be added to this bucket
    Bucket(Object key, int maxDoc, int parentSize) {
      docSet = new BitDocSet(new FixedBitSet(maxDoc)); // TODO FixedBitSet is expensive
    }

    void addDoc(int doc) {
      docSet.add(doc);
    }

    DocSet getDocSet() {
      return docSet;
    }
  }
}
