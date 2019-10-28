package org.apache.solr.search.facet;

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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.IntFunction;

import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.response.BasicResultContext;
import org.apache.solr.response.ResultContext;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.search.DocList;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.DocSlice;
import org.apache.solr.search.Filter;
import org.apache.solr.search.FunctionQParser;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QueryUtils;
import org.apache.solr.search.ReturnFields;
import org.apache.solr.search.SolrReturnFields;
import org.apache.solr.search.SortSpec;
import org.apache.solr.search.SortSpecParsing;
import org.apache.solr.search.SyntaxError;
import org.apache.solr.search.ValueSourceParser;


public class TopDocsAgg extends AggValueSource {
  Object qArg;
  int limit;
  Object sortArg;
  Object fieldsArg;

  public TopDocsAgg(Object q, int offset, int limit, Object sort, Object fields) {
    super("topdocs");
    this.qArg = q;
    this.limit = limit;
    this.sortArg = sort;
    this.fieldsArg = fields;
    // TODO store and use offset, or drop offset support for this use-case?
  }


  public static class Parser extends ValueSourceParser {
    @Override
    public ValueSource parse(FunctionQParser fp) throws SyntaxError {
      Object qArg = null;
      int offset = 0;
      int limit = 1;  // by default, just the top matching doc
      Object sortArg = null;
      Object fieldsArg = null;

      if (fp.hasMoreArguments()) {
        qArg = fp.parseArg();
      }
      if (fp.hasMoreArguments()) {
        offset = fp.parseInt();
      }
      if (fp.hasMoreArguments()) {
        limit = fp.parseInt();
      }
      if (fp.hasMoreArguments()) {
        sortArg = fp.parseArg();
      }
      if (fp.hasMoreArguments()) {
        fieldsArg = fp.parseArg();
      }

      return new TopDocsAgg(qArg, offset, limit, sortArg, fieldsArg);
    }
  }

  // TODO: make base class for parsers of Aggs
  public static class AggParser {
    public static AggValueSource parse(Object arg) {
      Object qArg = null;
      int offset = 0;
      int limit = 1;  // by default, just the top matching doc
      Object sortArg = null;
      Object fieldsArg = null;

      if (arg != null) {
        if (arg instanceof String) {
          qArg = (String)arg;
        } else if (arg instanceof Map) {
          Map<String,Object> map = (Map<String,Object>)arg;
          for (Map.Entry<String,Object> entry : map.entrySet()) {
            String key = entry.getKey();
            Object val = entry.getValue();
            // match the params of the JSON Request API for a query
            // "filter" isn't needed since we're starting with a domain that has already been filtered
            if ("query".equals(key)) {
              qArg = val;
            } else if ("offset".equals(key)) {
              offset = ((Number)val).intValue();
            } else if ("limit".equals(key)) {
              limit = ((Number)val).intValue();
            } else if ("sort".equals(key)) {
              sortArg = val;  // TODO: handle array of strings, array of maps?
            } else if ("fields".equals(key)) {
              fieldsArg = val;
            } else if ("type".equals(key)) {
              // OK
            } else {
              throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Unknown key '" + key + "' in topdocs aggregation: " + arg);
            }
          }
        }
      }

      return new TopDocsAgg(qArg, offset, limit, sortArg, fieldsArg);
    }
  }



  @Override
  public SlotAcc createSlotAcc(FacetContext fcontext, int numDocs, int numSlots) throws IOException {
    ResponseBuilder rb = SolrRequestInfo.getRequestInfo().getResponseBuilder();

    Query query = null;
    ReturnFields returnFields = null;

    if (qArg == null) {
      // use main query if query is not specified
      query = rb.getQuery();
    } else if (qArg instanceof String) {
      try {
        QParser qparser = QParser.getParser((String)qArg, null, fcontext.req);
        query = qparser.getQuery();
      } catch (SyntaxError syntaxError) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, syntaxError);
      }
    } else if (qArg instanceof Query) {
      query = (Query)qArg;
    } else {
      // TODO - JSON
    }

    Sort sort = buildSort(fcontext.req, rb);

    if (fieldsArg == null && fcontext.isShard()) { // shard requests normally only ask for fl=id,score
      returnFields = new SolrReturnFields();
    } else {
      returnFields = buildReturnFields(fcontext.req, rb.rsp);
    }

    // Validate sort fields are in return fields
    if (sort != null && !returnFields.wantsAllFields()) {
      for (SortField sortField : sort.getSort()) {
        if (!returnFields.wantsField(sortField.getField())) {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
              "topdocs sort field must be in returned field list - cannot sort by " + sortField.getField());
        }
      }
    }

    // This method won't work with post-filters that actually change the score, or change which documents match.
    // That may be desirable though.

    Sort sort2 = fcontext.searcher.weightSort(sort);
    Query query2 = QueryUtils.makeQueryable(query);
    // rewrite once so we won't do it per-bucket
    query2 = query2.rewrite(fcontext.searcher.getIndexReader());

    // todo: only create weight once?

    return new Acc(fcontext, query2, sort2, returnFields, numSlots);
  }

  private ReturnFields buildReturnFields(SolrQueryRequest req, SolrQueryResponse rsp) {
    if (fieldsArg == null) {
      return rsp.getReturnFields();
    } else if (fieldsArg instanceof String) {
      return new SolrReturnFields((String)fieldsArg, req);
    } else {
      String[] fl = null;
      if (fieldsArg instanceof List) {
        List lst = (List)fieldsArg;
        fl = (String[])lst.toArray(new String[lst.size()]);
      } else {
        fl = (String[])fieldsArg;
      }
      return new SolrReturnFields(fl, req);
    }
  }

  private Sort buildSort(SolrQueryRequest req, ResponseBuilder rb) {
    if (sortArg == null) {
      return rb.getSortSpec().getSort();
    } else if (sortArg instanceof Sort) {
      return (Sort)sortArg;
    } else if (sortArg instanceof String) {
      SortSpec ss = SortSpecParsing.parseSortSpec((String)sortArg, req);
      return ss.getSort();
    } else {
      // TODO - JSON
      return null;
    }
  }

  @Override
  public FacetMerger createFacetMerger(Object prototype) {
    return new Merger();
  }

  @Override
  public int hashCode() {
    // TODO: be careful if/how this is cached... queries can depend on the context (param substitution, NOW, etc)
    int h = qArg == null ? 29 : qArg.hashCode();
    h = h * 31 + limit;
    h = h * 31 + (sortArg == null ? 29 : sortArg.hashCode());
    return h;
  }

  @Override
  public String description() {
    return "topdocs"; // TODO
  }


  private class Acc extends SlotAcc {
    Query query;
    Sort sort;
    boolean doScores = true;
    ReturnFields returnFields;
    ResultContext[] result;

    Acc(FacetContext fcontext, Query query, Sort sort, ReturnFields returnFields, int numSlots) {
      super(fcontext);
      this.query = query;
      this.sort = sort;
      this.returnFields = returnFields;
      this.result = new ResultContext[numSlots];
    }

    @Override
    public void collect(int doc, int slot, IntFunction<SlotContext> slotContext) throws IOException {
      // We don't really care about individual docs, we're just going to build the whole result (once)
      if (result[slot] == null) {
        buildResultForSlot(slotContext.apply(slot).getSlotQuery(), slot);
      }
    }

    @Override
    public int collect(DocSet docs, int slot, IntFunction<SlotContext> slotContext) throws IOException {
      assert null == result[slot];
      return buildResultForSlot(docs.getTopFilter(), slot);
    }

    private int buildResultForSlot(Query slotQuery, int slot) throws IOException {
      BooleanQuery.Builder builder = new BooleanQuery.Builder();
      builder.add(query, BooleanClause.Occur.MUST);
      if (slotQuery != null) {
        builder.add(slotQuery, BooleanClause.Occur.FILTER);
      }
      builder.add(fcontext.base.getTopFilter(), BooleanClause.Occur.FILTER);
      Query finalQuery = builder.build();

      if (sort == null) sort = Sort.RELEVANCE;

      int offset = 0;
      TopDocs topDocs = fcontext.searcher.search(finalQuery, limit, sort);

      long totalHits = topDocs.totalHits.value;
      int nDocsReturned = topDocs.scoreDocs.length;
      float maxScore = totalHits > 0 ? topDocs.scoreDocs[0].score : 0.0f;
      int[] ids = new int[nDocsReturned];
      float scores[] = doScores ? new float[nDocsReturned] : null;
      for (int i=0; i<nDocsReturned; i++) {
        ScoreDoc scoreDoc = topDocs.scoreDocs[i];
        ids[i] = scoreDoc.doc;
        if (scores != null) scores[i] = scoreDoc.score;
      }
      DocList docList = new DocSlice(offset, ids.length, ids, scores, totalHits, maxScore, TotalHits.Relation.EQUAL_TO);

      result[slot] = new BasicResultContext(docList, returnFields , fcontext.searcher, query, fcontext.req);

      return (int)topDocs.totalHits.value;
    }

    @Override
    public int compare(int slotA, int slotB) {
      return slotA - slotB;
    }

    @Override
    public Object getValue(int slotNum) {
      return result[slotNum];
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



  public class Merger extends FacetMerger {
    SolrDocumentList result = new SolrDocumentList();
    List<SolrDocument> combinedDocs = new ArrayList<>(); // TODO use a priority queue

    @Override
    public void merge(Object facetResult, Context mcontext) {
      SolrDocumentList shardTopDocs = (SolrDocumentList)facetResult;
      result.setNumFound(result.getNumFound() + shardTopDocs.getNumFound());

      if (shardTopDocs.getMaxScore() != null) {
        result.setMaxScore(result.getMaxScore()==null ?
            shardTopDocs.getMaxScore()
            : Math.max(result.getMaxScore(), shardTopDocs.getMaxScore()));
      }

      combinedDocs.addAll(shardTopDocs);
    }

    @Override
    public void finish(Context mcontext) {
      // unused
    }

    @Override
    public Object getMergedResult() {

      SolrRequestInfo requestInfo = SolrRequestInfo.getRequestInfo();
      Sort sort = buildSort(requestInfo.getReq(), requestInfo.getResponseBuilder());
      if (sort != null) {
        ReturnFields returnFields = buildReturnFields(requestInfo.getReq(), requestInfo.getRsp());
        Map<String, String> renames = returnFields.getFieldRenames();
        SortField[] sortFields = sort.getSort();
        Comparator<Comparable> fieldComparator = Comparator.<Comparable>nullsLast(Comparator.<Comparable>naturalOrder());

        Comparator<SolrDocument> docComparator = new Comparator<SolrDocument>() {
          @Override
          public int compare(SolrDocument doc1, SolrDocument doc2) {
            for (int i = 0; i<sortFields.length; i++) {
              String originalSortName = sortFields[i].getField();
              String finalSortFieldName = renames.getOrDefault(originalSortName, originalSortName);
              int comparisonValue = fieldComparator.compare(
                  (Comparable)doc1.get(finalSortFieldName),
                  (Comparable)doc2.get(finalSortFieldName)
              );
              if (comparisonValue != 0) {
                int directionFactor = sortFields[i].getReverse() ? -1 : 1;
                return directionFactor * comparisonValue;
              }
            }
            return 0;
          }
        };

        Collections.sort(combinedDocs, docComparator);
      }

      result.addAll(combinedDocs.size() > limit ? combinedDocs.subList(0, limit) : combinedDocs);

      return result;
    }
  }
}

