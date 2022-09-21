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
package org.apache.solr.update.processor;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.component.RealTimeGetComponent;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.UpdateCommand;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.SolrException.ErrorCode.SERVER_ERROR;
import static org.apache.solr.update.processor.DistributingUpdateProcessorFactory.DISTRIB_UPDATE_PARAM;

public class ConditionalUpsertProcessorFactory extends UpdateRequestProcessorFactory implements SolrCoreAware, UpdateRequestProcessorFactory.RunAlways {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String PARAM_IGNORE_CONDITIONAL_UPSERTS = "ignoreConditionalUpserts";

  private final List<UpsertCondition> conditions = new ArrayList<>();

  @Override
  public void init(NamedList args)  {
    conditions.clear();
    conditions.addAll(UpsertCondition.readConditions(args));
    super.init(args);
  }

  @Override
  public ConditionalUpsertUpdateProcessor getInstance(SolrQueryRequest req,
                                                      SolrQueryResponse rsp,
                                                      UpdateRequestProcessor next) {
    // Ensure the parameters are forwarded to the leader
    DistributedUpdateProcessorFactory.addParamToDistributedRequestWhitelist(req, PARAM_IGNORE_CONDITIONAL_UPSERTS);

    // there may be cases where we want to revert to "regular" behaviour without the conditional
    // upsert logic interfering
    boolean ignoreConditionalUpserts = req.getOriginalParams().getBool(PARAM_IGNORE_CONDITIONAL_UPSERTS, false);

    return new ConditionalUpsertUpdateProcessor(req, next, conditions, ignoreConditionalUpserts);
  }

  @Override
  public void inform(SolrCore core) {
    if (core.getUpdateHandler().getUpdateLog() == null) {
      throw new SolrException(SERVER_ERROR, "updateLog must be enabled.");
    }

    if (core.getLatestSchema().getUniqueKeyField() == null) {
      throw new SolrException(SERVER_ERROR, "schema must have uniqueKey defined.");
    }
  }

  static class ConditionalUpsertUpdateProcessor extends UpdateRequestProcessor {
    private final SolrCore core;

    private DistributedUpdateProcessor distribProc;  // the distributed update processor following us
    private DistributedUpdateProcessor.DistribPhase phase;
    private final List<UpsertCondition> conditions;
    private final boolean ignoreConditionalUpserts;

    ConditionalUpsertUpdateProcessor(SolrQueryRequest req,
                                     UpdateRequestProcessor next,
                                     List<UpsertCondition> conditions,
                                     boolean ignoreConditionalUpserts) {
      super(next);
      this.core = req.getCore();
      this.conditions = conditions;
      this.ignoreConditionalUpserts = ignoreConditionalUpserts;

      for (UpdateRequestProcessor proc = next ;proc != null; proc = proc.next) {
        if (proc instanceof DistributedUpdateProcessor) {
          distribProc = (DistributedUpdateProcessor)proc;
          break;
        }
      }

      if (distribProc == null) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "DistributedUpdateProcessor must follow ConditionalUpsertProcessor");
      }

      phase = DistributedUpdateProcessor.DistribPhase.parseParam(req.getParams().get(DISTRIB_UPDATE_PARAM));
    }

    boolean isLeader(UpdateCommand cmd) {
      if ((cmd.getFlags() & (UpdateCommand.REPLAY | UpdateCommand.PEER_SYNC)) != 0) {
        return false;
      }
      if (phase == DistributedUpdateProcessor.DistribPhase.FROMLEADER) {
        return false;
      }
      distribProc.setupRequest(cmd);
      return distribProc.isLeader();
    }

    @Override
    public void processAdd(AddUpdateCommand cmd) throws IOException {
      if (!ignoreConditionalUpserts && !conditions.isEmpty() && isLeader(cmd)) {
        BytesRef indexedDocId = cmd.getIndexedId();
        SolrInputDocument oldDoc = RealTimeGetComponent.getInputDocument(
            core,
            indexedDocId,
            indexedDocId,
            null,
            null,
            RealTimeGetComponent.Resolution.DOC);
        SolrInputDocument newDoc = cmd.getSolrInputDocument();
        if (!UpsertCondition.shouldInsertOrUpsert(conditions, oldDoc, newDoc)) {
          return;
        }
      }
      super.processAdd(cmd);
    }
  }
}
