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

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CoreContainer;

public class TimTest {

  public static void main(String[] args) throws IOException, SolrServerException {
    File solrHome = new File("/Users/tim/Downloads/debugfacets");

    CoreContainer coreContainer = CoreContainer.createAndLoad(solrHome.toPath());

    //final SolrCore mentionsCore = coreContainer.create("mentions", new HashMap<>());

    EmbeddedSolrServer mentions = new EmbeddedSolrServer(coreContainer, "mentions");

    try {
      //String facet = args.length > 0 ? args[0] : "{ authors: { type:terms, field:lcauthor, method:dvstring, facet:{ aretweets: { type:query, q:\"url:twitter\" } } } }";
//      String facet = args.length > 0 ? args[0] : "{ authors: { type:terms, field:lcauthor, method:dvstring, facet:{ followers: { type:range, field:resource_twitter_author_metrics_follower_count, start:0, end:15, gap:3, facet: { tweets: { type:query, q:\"url:twitter\"} } } } } }";
//      String facet = args.length > 0 ? args[0] : "{ authors: { type:range, field:resource_twitter_author_metrics_follower_count, start:0, end:15, gap:5, facet: { names: { type:terms, field:lcauthor, method:dvstring /*, facet:{ tweets: { type:query, q:\"url:twitter\" } } */ } } } }";
      String facet = args.length > 0 ? args[0] : "{ authors: { type:range, field:resource_twitter_author_metrics_follower_count, start:0, end:15, gap:5, facet: { names: { type:terms, field:lcauthor, method:dvstring facet:{ maxthing:\"max(resource_twitter_author_metrics_follower_count)\"  } } } } }";
      String queryString = args.length > 1 ? args[1] : "*:*";
      SolrQuery query = new SolrQuery(queryString);
      query.setRows(0);
      query.add("json.facet", facet);
      QueryResponse rsp = mentions.query(query);

      System.out.println(rsp);

      NamedList<Object> facets = (NamedList<Object>) rsp.getResponse().get("facets");
      NamedList<Object> authors = (NamedList<Object>) facets.get("authors");
      List<Object> buckets = (List<Object>) authors.get("buckets");

      buckets.forEach(b -> {
        System.out.println(b.toString());
      });
    } finally {
      mentions.close();
      coreContainer.shutdown();
    }
  }

}
