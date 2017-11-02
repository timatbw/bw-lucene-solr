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
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
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
    SimpleOrderedMap<Object> result = new SimpleOrderedMap<>();
    result.add("buckets", buckets.values().stream().map(FacetBucket::getMergedBucket).collect(Collectors.toList()));
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
    System.err.println("TPO calling process() for " + freq.valueSource.description()
    + " and doc count is " + fcontext.base.size()
    + " and class is " + fcontext.base.getClass().getName()
    + " and maxDoc is " + fcontext.searcher.maxDoc());

    firstPhase = true;
    collect(fcontext.base, 0);
    // TODO sorting

    firstPhase = false;
    System.err.println("Bucket count is " + buckets.size());
    List<SimpleOrderedMap<Object>> bucketList = new ArrayList<>();
    for (Object key : buckets.keySet()) {
      Bucket bucket = buckets.get(key);
      SimpleOrderedMap<Object> bucketResponse = new SimpleOrderedMap<>();
      bucketResponse.add("val", key);
      System.err.println("Bucket key '" + key + "' has DocSet with " + bucket.docSet.size());
      // TODO send filter query
      fillBucket(bucketResponse, null, bucket.docSet, (fcontext.flags & FacetContext.SKIP_FACET)!=0, fcontext.facetInfo);
      // bucketResponse.add(key.toString(), buckets.docSet.size());
      bucketList.add(bucketResponse);
    }
    response = new SimpleOrderedMap<>();
    response.add("buckets", bucketList);
  }

  @Override
  void collect(int segDoc, int slot) throws IOException {
    if (firstPhase) {
      Object objectVal = functionValues.objectVal(segDoc);
//    System.err.println("Called collect with segDoc " + segDoc + " and got objectValue " + objectVal + " of type " + objectVal.getClass().getName());
      Bucket bucket = buckets.computeIfAbsent(objectVal, key -> new Bucket(key, fcontext.searcher.maxDoc()));
      bucket.docSet.add(segDoc + segBase);
    } else {
      super.collect(segDoc, slot);
    }
  }

  @Override
  void setNextReader(LeafReaderContext readerContext) throws IOException {
    if (firstPhase) {
      System.err.println("Given setNextReader with " + readerContext);
      segBase = readerContext.docBase;
      functionValues = freq.valueSource.getValues(fcontext.qcontext, readerContext);
    } else {
      super.setNextReader(readerContext);
    }
  }

  static class Bucket {
    DocSet docSet;

    Bucket(Object key, int maxSize) {
      docSet = new BitDocSet(new FixedBitSet(maxSize)); // TODO FixedBitSet is expensive
    }
  }
}
