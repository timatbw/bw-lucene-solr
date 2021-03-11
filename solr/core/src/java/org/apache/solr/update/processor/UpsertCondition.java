package org.apache.solr.update.processor;
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

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.lucene.search.BooleanClause;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.common.util.NamedList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.SolrException.ErrorCode.SERVER_ERROR;

class UpsertCondition {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final Pattern ACTION_PATTERN = Pattern.compile("^(skip|insert)|upsert:(\\*|[\\w,]+)$");
  private static final List<String> ALL_FIELDS = Collections.singletonList("*");

  private final String name;
  private final List<FieldRule> rules;
  private final Action action;
  private final List<String> upsertFields;

  UpsertCondition(String name, Action action, List<String> upsertFields, List<FieldRule> rules) {
    this.name = name;
    this.action = action;
    this.upsertFields = upsertFields;
    this.rules = rules;
  }

  static List<UpsertCondition> readConditions(NamedList args) {
    List<UpsertCondition> conditions = new ArrayList<>(args.size());
    for (Map.Entry<String, ?> entry: (NamedList<?>)args) {
      String name = entry.getKey();
      Object tmp = entry.getValue();
      if (tmp instanceof NamedList) {
        NamedList<String> condition = (NamedList<String>)tmp;
        conditions.add(UpsertCondition.parse(name, condition));
      } else {
        throw new SolrException(SERVER_ERROR, tmp + " not a valid upsert condition");
      }
    }
    return conditions;
  }

  static boolean shouldInsertOrUpsert(List<UpsertCondition> conditions, SolrInputDocument oldDoc, SolrInputDocument newDoc) {
    for (UpsertCondition condition: conditions) {
      if (condition.matches(oldDoc, newDoc)) {
        log.info("Condition {} matched, taking action", condition.getName());
        if (condition.isSkip()) {
          log.info("Condition {} matched - skipping insert", condition.getName());
          return false;
        }
        if (condition.isInsert()) {
          log.info("Condition {} matched - will insert", condition.getName());
          break;
        }

        condition.copyOldDocFields(oldDoc, newDoc);
        break;
      }
    }
    return true;
  }

  static UpsertCondition parse(String name, NamedList<String> args) {
    List<FieldRule> rules = new ArrayList<>();
    Action action = null;
    List<String> upsertFields = null;
    for (Map.Entry<String, String> entry: args) {
      String key = entry.getKey();
      if ("action".equals(key)) {
        String actionValue = entry.getValue();
        Matcher m = ACTION_PATTERN.matcher(actionValue);
        if (!m.matches()) {
          throw new SolrException(SERVER_ERROR, "'" + actionValue + "' not a valid action");
        }
        if (m.group(1) != null) {
          if ("skip".equals(m.group(1))) {
            action = Action.SKIP;
          } else {
            action = Action.INSERT;
          }
          upsertFields = null;
        } else {
          action = Action.UPSERT;
          String fields = m.group(2);
          upsertFields = Arrays.asList(fields.split(","));
        }
      } else {
        BooleanClause.Occur occur;
        try {
          occur = BooleanClause.Occur.valueOf(key.toUpperCase(Locale.ROOT));
        } catch(IllegalArgumentException e) {
          throw new SolrException(SERVER_ERROR, "'" + key + "' not a valid occurence value");
        }
        String value = entry.getValue();
        rules.add(FieldRule.parse(occur, value));
      }
    }
    if (action == null) {
      throw new SolrException(SERVER_ERROR, "no action defined for condition: " + name);
    }
    if (rules.isEmpty()) {
      throw new SolrException(SERVER_ERROR, "no rules specified for condition: " + name);
    }
    return new UpsertCondition(name, action, upsertFields, rules);
  }

  String getName() {
    return name;
  }

  boolean isSkip() {
    return action == Action.SKIP;
  }

  boolean isInsert() {
    return action == Action.INSERT;
  }

  void copyOldDocFields(SolrInputDocument oldDoc, SolrInputDocument newDoc) {
    if (action != Action.UPSERT) {
      throw new IllegalStateException("Can only copy old doc fields when upserting");
    }
    Collection<String> fieldsToCopy;
    if (ALL_FIELDS.equals(upsertFields)) {
      fieldsToCopy = oldDoc.keySet();
    } else {
      fieldsToCopy = upsertFields;
    }
    fieldsToCopy.forEach(field -> {
      if (!newDoc.containsKey(field)) {
        SolrInputField inputField = oldDoc.getField(field);
        newDoc.put(field, inputField);
      }
    });
  }

  boolean matches(SolrInputDocument oldDoc, SolrInputDocument newDoc) {
    Docs docs = new Docs(oldDoc, newDoc);
    boolean atLeastOneMatched = false;
    for (FieldRule rule: rules) {
      boolean ruleMatched = rule.matches(docs);
      switch(rule.getOccur()) {
        case MUST:
          if (!ruleMatched) {
            return false;
          }
          atLeastOneMatched = true;
          break;
        case MUST_NOT:
          if (ruleMatched) {
            return false;
          }
          atLeastOneMatched = true;
          break;
        default:
          atLeastOneMatched = ruleMatched || atLeastOneMatched;
          break;
      }
    }
    return atLeastOneMatched;
  }

  enum Action {
    UPSERT,
    INSERT,
    SKIP;
  }

  private static class Docs {
    private final SolrInputDocument oldDoc;
    private final SolrInputDocument newDoc;

    Docs(SolrInputDocument oldDoc, SolrInputDocument newDoc) {
      this.oldDoc = oldDoc;
      this.newDoc = newDoc;
    }

    SolrInputDocument getOldDoc() {
      return oldDoc;
    }

    SolrInputDocument getNewDoc() {
      return newDoc;
    }
  }

  private static class FieldRule {
    private static final Pattern RULE_CONDITION_PATTERN = Pattern.compile("^(OLD|NEW)\\.(\\*|\\w+:(?:\\w+|\\*))$");

    private final BooleanClause.Occur occur;
    private final Function<Docs, SolrInputDocument> docGetter;
    private final Predicate<SolrInputDocument> docPredicate;

    private FieldRule(BooleanClause.Occur occur, Function<Docs, SolrInputDocument> docGetter, Predicate<SolrInputDocument> docPredicate) {
      this.occur = occur;
      this.docGetter = docGetter;
      this.docPredicate = docPredicate;
    }

    static FieldRule parse(BooleanClause.Occur occur, String condition) {
      Matcher m = RULE_CONDITION_PATTERN.matcher(condition);
      if (m.matches()) {
        String doc = m.group(1);
        String predicate = m.group(2);
        Function<Docs, SolrInputDocument> docGetter;
        if (doc.equalsIgnoreCase("OLD")) {
          docGetter = Docs::getOldDoc;
        } else {
          docGetter = Docs::getNewDoc;
        }

        Predicate<SolrInputDocument> docPredicate;
        if ("*".equals(predicate)) {
          docPredicate = Objects::nonNull;
        } else {
          String[] parts = predicate.split(":");
          String field = parts[0];
          String value = parts[1];
          if ("*".equals(value)) {
            docPredicate = forField(field, Objects::nonNull);
          } else {
            docPredicate = forField(field, value::equals);
          }
        }
        return new FieldRule(occur, docGetter, docPredicate);
      }
      throw new SolrException(SERVER_ERROR, "'" + condition + "' not a valid condition for rule");
    }

    BooleanClause.Occur getOccur() {
      return occur;
    }

    boolean matches(Docs docs) {
      SolrInputDocument doc = docGetter.apply(docs);
      return docPredicate.test(doc);
    }

    private static Predicate<SolrInputDocument> forField(String field, Predicate<Object> fieldPredicate) {
      return doc -> doc != null && fieldPredicate.test(doc.getFieldValue(field));
    }
  }
}
