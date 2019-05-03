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

import java.util.List;

import org.apache.solr.common.SolrException;

public class DivAgg extends CombiningAggValueSource {

  public DivAgg(List<AggValueSource> subAggs) {
    super("div", subAggs);
    if (subAggs.size() != 2) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "div aggregate requires exactly 2 arguments");
    }
  }

  @Override
  protected Double combine(List<Number> values) {
    if (values.size() != 2) {
      throw new IllegalStateException("div aggregate expects 2 values but has " + values.size());
    }
    return values.get(0).doubleValue() / values.get(1).doubleValue();
  }
}
