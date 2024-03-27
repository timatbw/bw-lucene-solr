package org.apache.solr.update.processor;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ListMultimap;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.ByteArrayUtf8CharSequence;
import org.apache.solr.common.util.NamedList;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

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

public class UpsertConditionTest {

  @Test(expected = SolrException.class)
  public void givenNoAction_whenParsingCondition() {
    NamedList<String> args = namedList(ImmutableListMultimap.of("must", "OLD.field:value"));
    UpsertCondition.parse("no-action", args);
  }

  @Test(expected = SolrException.class)
  public void givenInvalidMatchOccurrence_whenParsingCondition() {
    NamedList<String> args = namedList(ImmutableListMultimap.of(
        "maybe_might", "OLD.field:value",
        "action", "skip"
    ));
    UpsertCondition.parse("bad-occurrence", args);
  }

  @Test(expected = SolrException.class)
  public void givenNoRules_whenParsingCondition() {
    NamedList<String> args = namedList(ImmutableListMultimap.of("action", "skip"));
    UpsertCondition.parse("no-rules", args);
  }

  @Test(expected = SolrException.class)
  public void givenBadRuleDocPart_whenParsingCondition() {
    NamedList<String> args = namedList(ImmutableListMultimap.of(
        "must", "YOUNG.field:value",
        "action", "skip"
    ));
    UpsertCondition.parse("bad-rule", args);
  }

  @Test(expected = SolrException.class)
  public void givenNoDocPart_whenParsingCondition() {
    NamedList<String> args = namedList(ImmutableListMultimap.of(
        "must", "field:value",
        "action", "skip"
    ));
    UpsertCondition.parse("bad-rule", args);
  }

  @Test(expected = SolrException.class)
  public void givenNoValuePart_whenParsingCondition() {
    NamedList<String> args = namedList(ImmutableListMultimap.of(
        "must", "OLD.field",
        "action", "skip"
    ));
    UpsertCondition.parse("bad-rule", args);
  }

  @Test(expected = SolrException.class)
  public void givenBadAction_whenParsingCondition() {
    NamedList<String> args = namedList(ImmutableListMultimap.of(
        "must", "OLD.field:value",
        "action", "skippy"
    ));
    UpsertCondition.parse("bad-action", args);
  }

  @Test(expected = SolrException.class)
  public void givenBadUpsert_whenParsingCondition() {
    NamedList<String> args = namedList(ImmutableListMultimap.of(
        "must", "OLD.field:value",
        "action", "upsert:%^&"
    ));
    UpsertCondition.parse("bad-action", args);
  }

  @Test(expected = SolrException.class)
  public void givenBadNullify_whenParsingCondition() {
    NamedList<String> args = namedList(ImmutableListMultimap.of(
        "must", "OLD.field:value",
        "action", "nullify:*"
    ));
    UpsertCondition.parse("bad-action", args);
  }

  @Test
  public void givenNoOldDoc_whenMatching() {
    NamedList<String> args = new NamedList<>(ImmutableMap.of(
        "must", "OLD.field:value",
        "action", "skip"
    ));

    UpsertCondition condition = UpsertCondition.parse("skip-it", args);

    assertThat(condition.getName(), is("skip-it"));

    SolrInputDocument oldDoc = null;
    SolrInputDocument newDoc = new SolrInputDocument();

    assertFalse(condition.matches(oldDoc, newDoc));
  }

  @Test
  public void givenMultiValuedField_whenMatching() {
    NamedList<String> args = new NamedList<>(ImmutableMap.of(
        "must", "OLD.field:value",
        "action", "skip"
    ));

    UpsertCondition condition = UpsertCondition.parse("skip-it", args);

    assertThat(condition.getName(), is("skip-it"));

    SolrInputDocument oldDoc = new SolrInputDocument();
    SolrInputDocument newDoc = new SolrInputDocument();

    assertFalse(condition.matches(oldDoc, newDoc));

    oldDoc.addField("field", "other1");
    assertFalse(condition.matches(oldDoc, newDoc));

    oldDoc.addField("field", "value");
    assertTrue(condition.matches(oldDoc, newDoc));

    oldDoc.addField("field", "other2");
    assertTrue(condition.matches(oldDoc, newDoc));
  }

  @Test
  public void givenNumericField_whenMatching() {
    NamedList<String> args = new NamedList<>(ImmutableMap.of(
        "must", "OLD.field:123",
        "action", "skip"
    ));

    UpsertCondition condition = UpsertCondition.parse("skip-it", args);

    assertThat(condition.getName(), is("skip-it"));

    SolrInputDocument oldDoc = new SolrInputDocument();
    SolrInputDocument newDoc = new SolrInputDocument();

    assertFalse(condition.matches(oldDoc, newDoc));

    oldDoc.setField("field", 999);
    assertFalse(condition.matches(oldDoc, newDoc));

    oldDoc.setField("field", 123);
    assertTrue(condition.matches(oldDoc, newDoc));
  }

  @Test
  public void givenAtomicUpdateSet_whenMatching() {
    NamedList<String> args = new NamedList<>(ImmutableMap.of(
        "must", "NEW.field:value",
        "action", "skip"
    ));

    UpsertCondition condition = UpsertCondition.parse("skip-it", args);

    assertThat(condition.getName(), is("skip-it"));

    SolrInputDocument oldDoc = new SolrInputDocument();
    SolrInputDocument newDoc = new SolrInputDocument();

    newDoc.setField("field", Collections.singletonMap("set", "other1"));
    assertFalse(condition.matches(oldDoc, newDoc));

    newDoc.setField("field", Collections.singletonMap("set", "value"));
    assertTrue(condition.matches(oldDoc, newDoc));

    newDoc.setField("field", Collections.singletonMap("set", ImmutableList.of("value", "other2")));
    assertTrue(condition.matches(oldDoc, newDoc));

    newDoc.setField("field", Collections.singletonMap("set", ImmutableList.of("other1", "other2")));
    assertFalse(condition.matches(oldDoc, newDoc));
  }

  @Test
  public void givenAtomicUpdateAdd_whenMatching() {
    NamedList<String> args = new NamedList<>(ImmutableMap.of(
        "must", "NEW.field:value",
        "action", "skip"
    ));

    UpsertCondition condition = UpsertCondition.parse("skip-it", args);

    assertThat(condition.getName(), is("skip-it"));

    SolrInputDocument oldDoc = new SolrInputDocument();
    SolrInputDocument newDoc = new SolrInputDocument();

    newDoc.setField("field", Collections.singletonMap("add", "other1"));
    assertFalse(condition.matches(oldDoc, newDoc));

    newDoc.setField("field", Collections.singletonMap("add", "value"));
    assertTrue(condition.matches(oldDoc, newDoc));

    newDoc.setField("field", Collections.singletonMap("add", ImmutableList.of("value", "other2")));
    assertTrue(condition.matches(oldDoc, newDoc));

    newDoc.setField("field", Collections.singletonMap("add", ImmutableList.of("other1", "other2")));
    assertFalse(condition.matches(oldDoc, newDoc));
  }

  @Test
  public void givenAtomicUpdateRemove_whenMatching() {
    NamedList<String> args = new NamedList<>(ImmutableMap.of(
        "must", "NEW.field:value",
        "action", "skip"
    ));

    UpsertCondition condition = UpsertCondition.parse("skip-it", args);

    assertThat(condition.getName(), is("skip-it"));

    SolrInputDocument oldDoc = new SolrInputDocument();
    SolrInputDocument newDoc = new SolrInputDocument();

    oldDoc.setField("field", Collections.singletonMap("remove", "other1"));
    assertFalse(condition.matches(oldDoc, newDoc));

    newDoc.setField("field", Collections.singletonMap("remove", "value"));
    assertFalse(condition.matches(oldDoc, newDoc));

    newDoc.setField("field", Collections.singletonMap("remove", ImmutableList.of("value", "other2")));
    assertFalse(condition.matches(oldDoc, newDoc));

    newDoc.setField("field", Collections.singletonMap("remove", ImmutableList.of("other1", "other2")));
    assertFalse(condition.matches(oldDoc, newDoc));
  }

  @Test
  public void givenSingleMustClause_whenMatching() {
    NamedList<String> args = namedList(ImmutableListMultimap.of(
        "must", "OLD.field:value",
        "action", "skip"
    ));

    UpsertCondition condition = UpsertCondition.parse("skip-it", args);

    assertThat(condition.getName(), is("skip-it"));

    SolrInputDocument oldDoc = new SolrInputDocument();
    SolrInputDocument newDoc = new SolrInputDocument();

    assertFalse(condition.matches(oldDoc, newDoc));

    oldDoc.setField("field", "value");
    assertTrue(condition.matches(oldDoc, newDoc));

    oldDoc.setField("field", "not-value");
    assertFalse(condition.matches(oldDoc, newDoc));
  }

  @Test
  public void givenSingleShouldClause_whenMatching() {
    NamedList<String> args = namedList(ImmutableListMultimap.of(
        "should", "OLD.field:value",
        "action", "skip"
    ));

    UpsertCondition condition = UpsertCondition.parse("skip-it", args);

    assertThat(condition.getName(), is("skip-it"));

    SolrInputDocument oldDoc = new SolrInputDocument();
    SolrInputDocument newDoc = new SolrInputDocument();

    assertFalse(condition.matches(oldDoc, newDoc));

    oldDoc.setField("field", "value");
    assertTrue(condition.matches(oldDoc, newDoc));

    oldDoc.setField("field", "not-value");
    assertFalse(condition.matches(oldDoc, newDoc));
  }

  @Test
  public void givenSingleMustNotClause_whenMatching() {
    NamedList<String> args = namedList(ImmutableListMultimap.of(
        "must_not", "OLD.field:value",
        "action", "skip"
    ));

    UpsertCondition condition = UpsertCondition.parse("skip-it", args);

    assertThat(condition.getName(), is("skip-it"));

    SolrInputDocument oldDoc = new SolrInputDocument();
    SolrInputDocument newDoc = new SolrInputDocument();

    assertTrue(condition.matches(oldDoc, newDoc));

    oldDoc.setField("field", "value");
    assertFalse(condition.matches(oldDoc, newDoc));

    oldDoc.setField("field", "not-value");
    assertTrue(condition.matches(oldDoc, newDoc));
  }

  @Test
  public void givenSingleShouldAnyValueClause_whenMatching() {
    NamedList<String> args = namedList(ImmutableListMultimap.of(
        "should", "OLD.field:*",
        "action", "skip"
    ));

    UpsertCondition condition = UpsertCondition.parse("skip-it", args);

    assertThat(condition.getName(), is("skip-it"));

    SolrInputDocument oldDoc = new SolrInputDocument();
    SolrInputDocument newDoc = new SolrInputDocument();

    assertFalse(condition.matches(oldDoc, newDoc));

    oldDoc.setField("field", "value");
    assertTrue(condition.matches(oldDoc, newDoc));

    oldDoc.setField("field", "not-value");
    assertTrue(condition.matches(oldDoc, newDoc));
  }

  @Test
  public void givenSingleShouldDocExistsClause_whenMatching() {
    NamedList<String> args = namedList(ImmutableListMultimap.of(
        "should", "OLD.*",
        "action", "skip"
    ));

    UpsertCondition condition = UpsertCondition.parse("skip-it", args);

    assertThat(condition.getName(), is("skip-it"));

    SolrInputDocument oldDoc = new SolrInputDocument();
    SolrInputDocument newDoc = new SolrInputDocument();

    assertFalse(condition.matches(null, newDoc));

    assertTrue(condition.matches(oldDoc, newDoc));

    oldDoc.setField("field", "value");
    assertTrue(condition.matches(oldDoc, newDoc));
  }

  @Test
  public void givenMultipleMustClauses_whenMatching() {
    NamedList<String> args = namedList(ImmutableListMultimap.of(
        "must", "OLD.field1:value1",
        "must", "NEW.field2:value2",
        "action", "skip"
    ));

    UpsertCondition condition = UpsertCondition.parse("skip-it", args);

    assertThat(condition.getName(), is("skip-it"));

    SolrInputDocument oldDoc = new SolrInputDocument();
    SolrInputDocument newDoc = new SolrInputDocument();

    assertFalse(condition.matches(oldDoc, newDoc));

    oldDoc.setField("field1", "value1");
    assertFalse(condition.matches(oldDoc, newDoc));

    oldDoc.setField("field1", "value1");
    newDoc.setField("field2", "value2");
    assertTrue(condition.matches(oldDoc, newDoc));

    oldDoc.setField("field1", "not-value1");
    newDoc.setField("field2", "value2");
    assertFalse(condition.matches(oldDoc, newDoc));

    oldDoc.removeField("field1");
    assertFalse(condition.matches(oldDoc, newDoc));
  }

  @Test
  public void givenMultipleShouldClauses_whenMatching() {
    NamedList<String> args = namedList(ImmutableListMultimap.of(
        "should", "OLD.field1:value1",
        "should", "NEW.field2:value2",
        "action", "skip"
    ));

    UpsertCondition condition = UpsertCondition.parse("skip-it", args);

    assertThat(condition.getName(), is("skip-it"));

    SolrInputDocument oldDoc = new SolrInputDocument();
    SolrInputDocument newDoc = new SolrInputDocument();

    assertFalse(condition.matches(oldDoc, newDoc));

    oldDoc.setField("field1", "value1");
    assertTrue(condition.matches(oldDoc, newDoc));

    oldDoc.setField("field1", "value1");
    newDoc.setField("field2", "value2");
    assertTrue(condition.matches(oldDoc, newDoc));

    oldDoc.setField("field1", "not-value1");
    newDoc.setField("field2", "value2");
    assertTrue(condition.matches(oldDoc, newDoc));

    oldDoc.removeField("field1");
    assertTrue(condition.matches(oldDoc, newDoc));

    newDoc.setField("field2", "not-value2");
    assertFalse(condition.matches(oldDoc, newDoc));
  }

  @Test
  public void givenMultipleShouldAndMustNotClauses_whenMatching() {
    NamedList<String> args = namedList(ImmutableListMultimap.of(
        "should", "NEW.field1:*",
        "should", "NEW.field2:*",
        "must_not", "NEW.field3:*",
        "action", "skip"
    ));

    UpsertCondition condition = UpsertCondition.parse("skip-it", args);

    assertThat(condition.getName(), is("skip-it"));

    {
      SolrInputDocument newDoc = new SolrInputDocument();
      assertFalse(condition.matches(null, newDoc));
    }

    {
      SolrInputDocument newDoc = new SolrInputDocument();
      newDoc.setField("field3", "anything");
      assertFalse(condition.matches(null, newDoc));
    }

    {
      SolrInputDocument newDoc = new SolrInputDocument();
      newDoc.setField("field2", "anything");
      assertTrue(condition.matches(null, newDoc));
    }

    {
      SolrInputDocument newDoc = new SolrInputDocument();
      newDoc.setField("field1", "anything");
      newDoc.setField("field2", "anything-else");
      assertTrue(condition.matches(null, newDoc));
    }

    {
      SolrInputDocument newDoc = new SolrInputDocument();
      newDoc.setField("field2", "anything");
      newDoc.setField("field3", "stuff");
      assertFalse(condition.matches(null, newDoc));
    }

    {
      SolrInputDocument newDoc = new SolrInputDocument();
      newDoc.setField("field99", "anything");
      assertFalse(condition.matches(null, newDoc));
    }

    {
      SolrInputDocument newDoc = new SolrInputDocument();
      newDoc.setField("field99", "anything");
      newDoc.setField("field3", "stuff");
      assertFalse(condition.matches(null, newDoc));
    }
  }

  @Test
  public void givenMustAndMustNotClauses_whenMatching() {
    NamedList<String> args = namedList(ImmutableListMultimap.of(
        "must", "OLD.field1:value1",
        "must_not", "NEW.field2:value2",
        "action", "skip"
    ));

    UpsertCondition condition = UpsertCondition.parse("skip-it", args);

    assertThat(condition.getName(), is("skip-it"));

    SolrInputDocument oldDoc = new SolrInputDocument();
    SolrInputDocument newDoc = new SolrInputDocument();

    assertFalse(condition.matches(oldDoc, newDoc));

    oldDoc.setField("field1", "value1");
    assertTrue(condition.matches(oldDoc, newDoc));

    oldDoc.setField("field1", "value1");
    newDoc.setField("field2", "value2");
    assertFalse(condition.matches(oldDoc, newDoc));

    oldDoc.setField("field1", "not-value1");
    newDoc.setField("field2", "value2");
    assertFalse(condition.matches(oldDoc, newDoc));

    oldDoc.setField("field1", "value1");
    newDoc.setField("field2", "not-value2");
    assertTrue(condition.matches(oldDoc, newDoc));
  }

  @Test
  public void givenSkipAction_whenRunning() {
    NamedList<String> args = namedList(ImmutableListMultimap.of(
        "should", "OLD.field:value",
        "action", "skip"
    ));

    UpsertCondition condition = UpsertCondition.parse("skip-it", args);

    assertThat(condition.getName(), is("skip-it"));

    SolrInputDocument oldDoc = new SolrInputDocument();
    SolrInputDocument newDoc = new SolrInputDocument();

    assertThat(condition.run(oldDoc, newDoc), is(UpsertCondition.ActionType.SKIP));
    assertThat(oldDoc.isEmpty(), is(true));
    assertThat(newDoc.isEmpty(), is(true));
  }

  @Test
  public void givenInsertAction_whenRunning() {
    NamedList<String> args = namedList(ImmutableListMultimap.of(
        "should", "OLD.field:value",
        "action", "insert"
    ));

    UpsertCondition condition = UpsertCondition.parse("insert-it", args);

    assertThat(condition.getName(), is("insert-it"));

    SolrInputDocument oldDoc = new SolrInputDocument();
    SolrInputDocument newDoc = new SolrInputDocument();

    assertThat(condition.run(oldDoc, newDoc), is(UpsertCondition.ActionType.INSERT));
    assertThat(oldDoc.isEmpty(), is(true));
    assertThat(newDoc.isEmpty(), is(true));
  }

  @Test
  public void givenUpsertForSpecificFields_whenRunning() {
    NamedList<String> args = namedList(ImmutableListMultimap.of(
        "must", "OLD.field:value",
        "action", "upsert:field,other_field"
    ));

    UpsertCondition condition = UpsertCondition.parse("upsert", args);

    assertThat(condition.getName(), is("upsert"));

    SolrInputDocument oldDoc = new SolrInputDocument();
    SolrInputDocument newDoc = new SolrInputDocument();
    oldDoc.setField("field", "value");
    oldDoc.setField("other_field", "old-value");
    oldDoc.setField("not-copied", "not-copied");

    assertThat(condition.run(oldDoc, newDoc), is(UpsertCondition.ActionType.UPSERT));

    assertThat(newDoc.getFieldValue("field"), is("value"));
    assertThat(newDoc.getFieldValue("other_field"), is("old-value"));
    assertFalse(newDoc.containsKey("not-copied"));

    newDoc = new SolrInputDocument();
    newDoc.setField("field", "left-alone");

    assertThat(condition.run(oldDoc, newDoc), is(UpsertCondition.ActionType.UPSERT));

    assertThat(newDoc.getFieldValue("field"), is("left-alone"));
    assertThat(newDoc.getFieldValue("other_field"), is("old-value"));
    assertFalse(newDoc.containsKey("not-copied"));
  }

  @Test
  public void givenRetainForSpecificFields_whenRunning() {
    NamedList<String> args = namedList(ImmutableListMultimap.of(
        "must", "OLD.field:value",
        "action", "retain:field,other_field"
    ));

    UpsertCondition condition = UpsertCondition.parse("retain", args);

    assertThat(condition.getName(), is("retain"));

    SolrInputDocument oldDoc = new SolrInputDocument();
    SolrInputDocument newDoc = new SolrInputDocument();
    oldDoc.setField("field", "old-value1");
    oldDoc.setField("other_field", "old-value2");
    oldDoc.setField("not-copied", "not-copied");

    assertThat(condition.run(oldDoc, newDoc), is(UpsertCondition.ActionType.RETAIN));

    assertThat(newDoc.getFieldValue("field"), is("old-value1"));
    assertThat(newDoc.getFieldValue("other_field"), is("old-value2"));
    assertFalse(newDoc.containsKey("not-copied"));

    newDoc = new SolrInputDocument();
    newDoc.setField("field", "should-be-overridden");

    assertThat(condition.run(oldDoc, newDoc), is(UpsertCondition.ActionType.RETAIN));

    assertThat(newDoc.getFieldValue("field"), is("old-value1"));
    assertThat(newDoc.getFieldValue("other_field"), is("old-value2"));
    assertFalse(newDoc.containsKey("not-copied"));
  }

  @Test
  public void givenUpsertForAllFields_whenRunning() {
    NamedList<String> args = namedList(ImmutableListMultimap.of(
        "must", "OLD.field:value",
        "action", "upsert:*"
    ));

    UpsertCondition condition = UpsertCondition.parse("upsert", args);

    assertThat(condition.getName(), is("upsert"));

    SolrInputDocument oldDoc = new SolrInputDocument();
    SolrInputDocument newDoc = new SolrInputDocument();
    oldDoc.setField("field", "value");
    oldDoc.setField("other_field", "old-value");
    oldDoc.setField("also-copied", "also-copied");

    assertThat(condition.run(oldDoc, newDoc), is(UpsertCondition.ActionType.UPSERT));

    assertThat(newDoc.getFieldValue("field"), is("value"));
    assertThat(newDoc.getFieldValue("other_field"), is("old-value"));
    assertThat(newDoc.getFieldValue("also-copied"), is("also-copied"));

    newDoc = new SolrInputDocument();
    newDoc.setField("field", "left-alone");

    assertThat(condition.run(oldDoc, newDoc), is(UpsertCondition.ActionType.UPSERT));

    assertThat(newDoc.getFieldValue("field"), is("left-alone"));
    assertThat(newDoc.getFieldValue("other_field"), is("old-value"));
    assertThat(newDoc.getFieldValue("also-copied"), is("also-copied"));
  }

  @Test
  public void givenUpsertAndNoOldDoc_whenRunning() {
    NamedList<String> args = namedList(ImmutableListMultimap.of(
        "must", "NEW.field:value",
        "action", "upsert:field,other_field"
    ));

    UpsertCondition condition = UpsertCondition.parse("upsert", args);

    assertThat(condition.getName(), is("upsert"));

    SolrInputDocument newDoc = new SolrInputDocument();
    newDoc.setField("field", "left-alone");

    assertThat(condition.run(null, newDoc), is(UpsertCondition.ActionType.UPSERT));

    assertThat(newDoc.getFieldValue("field"), is("left-alone"));
  }

  @Test
  public void givenNullify_whenRunning() {
    NamedList<String> args = namedList(ImmutableListMultimap.of(
        "must", "OLD.field:value",
        "action", "nullify:field,other_field"
    ));

    UpsertCondition condition = UpsertCondition.parse("nullify", args);

    assertThat(condition.getName(), is("nullify"));

    SolrInputDocument oldDoc = new SolrInputDocument();
    SolrInputDocument newDoc = new SolrInputDocument();
    newDoc.setField("field", "value");
    newDoc.setField("other_field", "other-value");
    newDoc.setField("left-alone", "not-null");

    assertThat(condition.run(oldDoc, newDoc), is(UpsertCondition.ActionType.NULLIFY));

    assertThat(newDoc.getFieldValue("field"), nullValue());
    assertThat(newDoc.getField("field"), notNullValue());
    assertThat(newDoc.getFieldValue("other_field"), nullValue());
    assertThat(newDoc.getField("field"), notNullValue());
    assertThat(newDoc.getFieldValue("left-alone"), is("not-null"));
  }

  @Test
  public void givenConcat_whenRunning() {
    NamedList<String> args = namedList(ImmutableListMultimap.of(
        "must_not", "NEW.derived_field:*",
        "action", "concat:derived_field:field,other_field"
    ));

    UpsertCondition condition = UpsertCondition.parse("concat", args);

    assertThat(condition.getName(), is("concat"));

    {
      SolrInputDocument newDoc = new SolrInputDocument();
      newDoc.setField("field", "Red");
      newDoc.setField("other_field", "Blue");

      assertTrue(condition.matches(null, newDoc));
      assertThat(condition.run(null, newDoc), is(UpsertCondition.ActionType.CONCAT));

      assertThat(newDoc.getFieldValue("derived_field"), is("RedBlue"));
      assertThat(newDoc.getFieldValue("field"), is("Red"));
      assertThat(newDoc.getFieldValue("other_field"), is("Blue"));
    }

    {
      SolrInputDocument newDoc2 = new SolrInputDocument();
      newDoc2.setField("derived_field", "AlreadySet");
      assertFalse(condition.matches(null, newDoc2));
    }
  }

  @Test
  public void givenConcatLowercase_whenRunning() {
    NamedList<String> args = namedList(ImmutableListMultimap.of(
        "must_not", "NEW.derived_field:*",
        "action", "concat_lc:derived_field:field,other_field,maybe_third?"
    ));

    UpsertCondition condition = UpsertCondition.parse("concat", args);

    {
      SolrInputDocument docWithAllFields = new SolrInputDocument();
      docWithAllFields.setField("field", "Red");
      docWithAllFields.setField("other_field", "Blue");
      docWithAllFields.setField("maybe_third", "Green");

      assertTrue(condition.matches(null, docWithAllFields));
      assertThat(condition.run(null, docWithAllFields), is(UpsertCondition.ActionType.CONCAT_LC));

      assertThat(docWithAllFields.getFieldValue("derived_field"), is("redbluegreen"));
      assertThat(docWithAllFields.getFieldValue("field"), is("Red"));
      assertThat(docWithAllFields.getFieldValue("other_field"), is("Blue"));
      assertThat(docWithAllFields.getFieldValue("maybe_third"), is("Green"));
    }

    {
      SolrInputDocument docAlreadySetsDerived = new SolrInputDocument();
      docAlreadySetsDerived.setField("derived_field", "AlreadySet");
      assertFalse(condition.matches(null, docAlreadySetsDerived));
    }

    {
      SolrInputDocument docWithoutOptional = new SolrInputDocument();
      docWithoutOptional.setField("field", "Yellow");
      docWithoutOptional.setField("other_field", "Orange");

      assertTrue(condition.matches(null, docWithoutOptional));
      assertThat(condition.run(null, docWithoutOptional), is(UpsertCondition.ActionType.CONCAT_LC));
      assertThat(docWithoutOptional.getFieldValue("derived_field"), is("yelloworange"));
    }

    {
      SolrInputDocument docWithoutRequired = new SolrInputDocument();
      docWithoutRequired.setField("field", "Yellow");

      assertTrue(condition.matches(null, docWithoutRequired));
      assertThat(condition.run(null, docWithoutRequired), is(UpsertCondition.ActionType.CONCAT_LC));
      assertThat(docWithoutRequired.getFieldValue("derived_field"), nullValue());
    }
  }

  @Test
  public void givenConcatWithFallbacks_whenRunning() {
    NamedList<String> args = namedList(ImmutableListMultimap.of(
        "must_not", "NEW.derived_field:*",
        "action", "concat:derived_field:maybe_prefix?,main_field|fallback_field|last_resort,maybe_suffix?"
    ));

    UpsertCondition condition = UpsertCondition.parse("concat", args);

    {
      SolrInputDocument mainPresent = new SolrInputDocument();
      mainPresent.setField("main_field", "Red");
      mainPresent.setField("last_resort", "Blue");
      assertTrue(condition.matches(null, mainPresent));
      assertThat(condition.run(null, mainPresent), is(UpsertCondition.ActionType.CONCAT));
      assertThat(mainPresent.getFieldValue("derived_field"), is("Red"));
    }

    {
      SolrInputDocument mainAbsent = new SolrInputDocument();
      mainAbsent.setField("fallback_field", "Green");
      mainAbsent.setField("last_resort", "Blue");
      assertTrue(condition.matches(null, mainAbsent));
      assertThat(condition.run(null, mainAbsent), is(UpsertCondition.ActionType.CONCAT));
      assertThat(mainAbsent.getFieldValue("derived_field"), is("Green"));
    }

    {
      SolrInputDocument onlyLast = new SolrInputDocument();
      onlyLast.setField("last_resort", "Blue");
      assertTrue(condition.matches(null, onlyLast));
      assertThat(condition.run(null, onlyLast), is(UpsertCondition.ActionType.CONCAT));
      assertThat(onlyLast.getFieldValue("derived_field"), is("Blue"));
    }

    {
      SolrInputDocument preAndPost = new SolrInputDocument();
      preAndPost.setField("fallback_field", "Green");
      preAndPost.setField("maybe_prefix", "pre");
      preAndPost.setField("maybe_suffix", "post");
      assertTrue(condition.matches(null, preAndPost));
      assertThat(condition.run(null, preAndPost), is(UpsertCondition.ActionType.CONCAT));
      assertThat(preAndPost.getFieldValue("derived_field"), is("preGreenpost"));
    }

    {
      SolrInputDocument onlyPreAndPost = new SolrInputDocument();
      onlyPreAndPost.setField("maybe_prefix", "pre");
      onlyPreAndPost.setField("maybe_suffix", "post");
      assertTrue(condition.matches(null, onlyPreAndPost));
      assertThat(condition.run(null, onlyPreAndPost), is(UpsertCondition.ActionType.CONCAT));
      assertThat(onlyPreAndPost.getFieldValue("derived_field"), nullValue());
    }
  }

  @Test
  public void givenConcatWithOldDoc_whenRunning() {
    NamedList<String> args = namedList(ImmutableListMultimap.of(
        "must_not", "NEW.sku:*",
        "action", "concat:sku:model_name|product_range,colour,size?"
    ));

    UpsertCondition condition = UpsertCondition.parse("concat", args);

    {
      SolrInputDocument newDoc = new SolrInputDocument();
      newDoc.setField("model_name", "Macbook");
      SolrInputDocument oldDoc = new SolrInputDocument();
      oldDoc.setField("model_name", "Powerbook");
      oldDoc.setField("colour", "Silver");
      oldDoc.setField("sku", "PowerbookSilver");
      assertTrue(condition.matches(oldDoc, newDoc));
      assertThat(condition.run(oldDoc, newDoc), is(UpsertCondition.ActionType.CONCAT));
      assertThat(newDoc.getFieldValue("sku"), is("MacbookSilver"));
    }

    {
      // Same test but simulating how the javabin format encodes strings
      SolrInputDocument newDoc = new SolrInputDocument();
      newDoc.setField("model_name", new ByteArrayUtf8CharSequence("Macbook"));
      SolrInputDocument oldDoc = new SolrInputDocument();
      oldDoc.setField("model_name", "Powerbook");
      oldDoc.setField("colour", "Silver");
      oldDoc.setField("sku", "PowerbookSilver");
      assertTrue(condition.matches(oldDoc, newDoc));
      assertThat(condition.run(oldDoc, newDoc), is(UpsertCondition.ActionType.CONCAT));
      assertThat(newDoc.getFieldValue("sku"), is("MacbookSilver"));
    }

    {
      SolrInputDocument newDoc = new SolrInputDocument();
      newDoc.setField("model_name", "Macbook");
      newDoc.setField("sku", "CustomOverride");
      SolrInputDocument oldDoc = new SolrInputDocument();
      oldDoc.setField("model_name", "Powerbook");
      oldDoc.setField("colour", "Black");
      oldDoc.setField("sku", "PowerbookBlack");
      assertFalse(condition.matches(oldDoc, newDoc));
    }

    {
      SolrInputDocument newDoc = new SolrInputDocument();
      newDoc.setField("colour", "Grey");
      newDoc.setField("product_range", "Laptop");
      SolrInputDocument oldDoc = new SolrInputDocument();
      oldDoc.setField("model_name", "Powerbook");
      oldDoc.setField("colour", "Silver");
      oldDoc.setField("size", "13in");
      oldDoc.setField("sku", "PowerbookSilver13in");
      assertTrue(condition.matches(oldDoc, newDoc));
      assertThat(condition.run(oldDoc, newDoc), is(UpsertCondition.ActionType.CONCAT));
      assertThat(newDoc.getFieldValue("sku"), is("PowerbookGrey13in"));
      // prefers old.model_name to new.product_range fallback
    }

    {
      SolrInputDocument newDoc = new SolrInputDocument();
      newDoc.setField("product_range", "Laptop");
      newDoc.setField("size", "16in");
      SolrInputDocument oldDoc = new SolrInputDocument();
      oldDoc.setField("colour", "Silver");
      oldDoc.setField("size", "17in");
      oldDoc.setField("sku", "PowerbookSilver17in");
      assertTrue(condition.matches(oldDoc, newDoc));
      assertThat(condition.run(oldDoc, newDoc), is(UpsertCondition.ActionType.CONCAT));
      assertThat(newDoc.getFieldValue("sku"), is("LaptopSilver16in"));
      // fallback to new.product_range since model_name unavailable in old and new
    }

    {
      SolrInputDocument newDoc = new SolrInputDocument();
      newDoc.setField("colour", "Grey");
      SolrInputDocument oldDoc = new SolrInputDocument();
      oldDoc.setField("product_range", "Laptop");
      oldDoc.setField("colour", "Silver");
      oldDoc.setField("size", "17in");
      oldDoc.setField("sku", "PowerbookSilver17in");
      assertTrue(condition.matches(oldDoc, newDoc));
      assertThat(condition.run(oldDoc, newDoc), is(UpsertCondition.ActionType.CONCAT));
      assertThat(newDoc.getFieldValue("sku"), is("LaptopGrey17in"));
      // fallback to old.product_range since model_name unavailable in old and new
    }

    {
      SolrInputDocument newDoc = new SolrInputDocument();
      newDoc.setField("size", "16in");
      SolrInputDocument oldDoc = new SolrInputDocument();
      oldDoc.setField("colour", "Silver");
      oldDoc.setField("size", "17in");
      oldDoc.setField("sku", "PowerbookSilver17in");
      assertTrue(condition.matches(oldDoc, newDoc));
      assertThat(condition.run(oldDoc, newDoc), is(UpsertCondition.ActionType.CONCAT));
      assertThat(newDoc.getFieldValue("sku"), nullValue());
      // both product_range and model_name unavailable in old and new
    }
  }

  @Test
  public void givenConcatWithAtomicUpdates_whenRunning() {
    NamedList<String> args = namedList(ImmutableListMultimap.<String, String>builder()
        .put("should", "NEW.model_name:*")
        .put("should", "NEW.product_range:*")
        .put("should", "NEW.colour:*")
        .put("should", "NEW.size:*")
        .put("must_not", "NEW.sku:*")
        .put("action", "concat:sku:model_name|product_range,colour,size?")
        .build()
    );

    UpsertCondition condition = UpsertCondition.parse("concat", args);

    {
      SolrInputDocument newDoc = new SolrInputDocument();
      newDoc.setField("model_name", Collections.singletonMap("set", "Macbook"));
      SolrInputDocument oldDoc = new SolrInputDocument();
      oldDoc.setField("model_name", "Powerbook");
      oldDoc.setField("colour", "Silver");
      oldDoc.setField("sku", "PowerbookSilver");
      assertTrue(condition.matches(oldDoc, newDoc));
      assertThat(condition.run(oldDoc, newDoc), is(UpsertCondition.ActionType.CONCAT));
      assertThat(newDoc.getFieldValue("sku"), is("MacbookSilver"));
    }

    {
      // Same test but simulating how the javabin format encodes strings
      SolrInputDocument newDoc = new SolrInputDocument();
      newDoc.setField("model_name",
          Collections.singletonMap("set", new ByteArrayUtf8CharSequence("Macbook")));
      SolrInputDocument oldDoc = new SolrInputDocument();
      oldDoc.setField("model_name", "Powerbook");
      oldDoc.setField("colour", "Silver");
      oldDoc.setField("sku", "PowerbookSilver");
      assertTrue(condition.matches(oldDoc, newDoc));
      assertThat(condition.run(oldDoc, newDoc), is(UpsertCondition.ActionType.CONCAT));
      assertThat(newDoc.getFieldValue("sku"), is("MacbookSilver"));
    }

    {
      SolrInputDocument newDoc = new SolrInputDocument();
      newDoc.setField("unrelated_field", Collections.singletonMap("set", "English"));
      newDoc.setField("size", Collections.singletonMap("set", "12in"));
      SolrInputDocument oldDoc = new SolrInputDocument();
      oldDoc.setField("model_name", "Powerbook");
      oldDoc.setField("colour", "Silver");
      oldDoc.setField("sku", "PowerbookSilver");
      assertTrue(condition.matches(oldDoc, newDoc));
      assertThat(condition.run(oldDoc, newDoc), is(UpsertCondition.ActionType.CONCAT));
      assertThat(newDoc.getFieldValue("sku"), is("PowerbookSilver12in"));
    }

    {
      SolrInputDocument newDoc = new SolrInputDocument();
      newDoc.setField("unrelated_field", Collections.singletonMap("set", "English"));
      SolrInputDocument oldDoc = new SolrInputDocument();
      oldDoc.setField("model_name", "Powerbook");
      oldDoc.setField("colour", "Silver");
      oldDoc.setField("sku", "PowerbookSilver");
      assertFalse(condition.matches(oldDoc, newDoc));
    }

    {
      SolrInputDocument newDoc = new SolrInputDocument();
      newDoc.setField("size", Collections.singletonMap("set", "12in"));
      SolrInputDocument oldDoc = new SolrInputDocument();
      oldDoc.setField("model_name", "Powerbook");
      oldDoc.setField("sku", "PowerbookSilver");
      assertTrue(condition.matches(oldDoc, newDoc));
      assertThat(condition.run(oldDoc, newDoc), is(UpsertCondition.ActionType.CONCAT));
      assertThat(newDoc.getFieldValue("sku"), nullValue());
    }
  }

  @Test
  public void givenConcatWithMultipleConditions_whenRunning() {
    NamedList<?> args = namedList(ImmutableListMultimap.<String, NamedList<String>>builder()
        .put("modelBased", namedList(ImmutableListMultimap.of(
            "must_not", "NEW.sku:*",
            "action", "concat:sku:model_name,colour,size?"
        )))
        .put("productBased", namedList(ImmutableListMultimap.of(
            "must_not", "NEW.sku:*",
            "action", "concat:sku:product_range,colour,size?"
        )))
        .build()
    );

    final List<UpsertCondition> conditions = UpsertCondition.readConditions(args);

    {
      SolrInputDocument newDoc = new SolrInputDocument();
      newDoc.setField("model_name", Collections.singletonMap("set", "Macbook"));
      newDoc.setField("product_range", Collections.singletonMap("set", "Laptop"));
      SolrInputDocument oldDoc = new SolrInputDocument();
      oldDoc.setField("model_name", "Powerbook");
      oldDoc.setField("colour", "Silver");
      oldDoc.setField("sku", "PowerbookSilver");
      assertThat(UpsertCondition.shouldInsertOrUpsert(conditions, oldDoc, newDoc), is(true));
      assertThat(newDoc.getFieldValue("sku"), is("MacbookSilver"));
      // does not go on to set sku using product_range because new.sku is set from first condition
    }

    {
      SolrInputDocument newDoc = new SolrInputDocument();
      newDoc.setField("product_range", Collections.singletonMap("set", "Laptop"));
      SolrInputDocument oldDoc = new SolrInputDocument();
      oldDoc.setField("colour", "Silver");
      oldDoc.setField("sku", "PowerbookSilver");
      assertThat(UpsertCondition.shouldInsertOrUpsert(conditions, oldDoc, newDoc), is(true));
      assertThat(newDoc.getFieldValue("sku"), is("LaptopSilver"));
      // first condition is not able to actually set new.sku so second condition matches
    }

    {
      SolrInputDocument newDoc = new SolrInputDocument();
      newDoc.setField("colour", Collections.singletonMap("set", "Black"));
      SolrInputDocument oldDoc = new SolrInputDocument();
      oldDoc.setField("colour", "Silver");
      oldDoc.setField("sku", "PowerbookSilver");
      assertThat(UpsertCondition.shouldInsertOrUpsert(conditions, oldDoc, newDoc), is(true));
      assertThat(newDoc.getFieldValue("sku"), nullValue());
      // neither condition was able to actually set new.sku
    }
  }

  @Test
  public void givenExistingPermanentDelete_whenCheckingShouldInsertOrUpsert() {
    List<UpsertCondition> conditions = givenMultipleConditions();

    SolrInputDocument oldDoc = new SolrInputDocument();
    SolrInputDocument newDoc = new SolrInputDocument();

    oldDoc.setField("compliance_reason", "delete");

    assertThat(UpsertCondition.shouldInsertOrUpsert(conditions, oldDoc, newDoc), is(false));
  }

  @Test
  public void givenExistingSoftDelete_whenCheckingShouldInsertOrUpsert() {
    List<UpsertCondition> conditions = givenMultipleConditions();

    SolrInputDocument oldDoc = new SolrInputDocument();
    SolrInputDocument newDoc = new SolrInputDocument();

    oldDoc.setField("compliance_reason", "soft_delete");
    oldDoc.setField("old_field", "not-kept-from-old");
    newDoc.setField("new_field", "kept-from-new");

    assertThat(UpsertCondition.shouldInsertOrUpsert(conditions, oldDoc, newDoc), is(true));
    assertThat(newDoc.getFieldValue("new_field"), is("kept-from-new"));
    assertThat(newDoc.getFieldValue("compliance_reason"), is("soft_delete"));
    assertThat(newDoc.getFieldValue("old_field"), nullValue());
  }

  @Test
  public void givenNewSoftDelete_whenCheckingShouldInsertOrUpsert() {
    List<UpsertCondition> conditions = givenMultipleConditions();

    SolrInputDocument oldDoc = new SolrInputDocument();
    SolrInputDocument newDoc = new SolrInputDocument();

    oldDoc.setField("old_field1", "kept-from-old1");
    oldDoc.setField("old_field2", "kept-from-old2");
    newDoc.setField("compliance_reason", "soft_delete");

    assertThat(UpsertCondition.shouldInsertOrUpsert(conditions, oldDoc, newDoc), is(true));
    assertThat(newDoc.getFieldValue("old_field1"), is("kept-from-old1"));
    assertThat(newDoc.getFieldValue("old_field2"), is("kept-from-old2"));
    assertThat(newDoc.getFieldValue("compliance_reason"), is("soft_delete"));
  }

  @Test
  public void givenExistingMetrics_whenCheckingShouldInsertOrUpsert() {
    List<UpsertCondition> conditions = givenMultipleConditions();

    SolrInputDocument oldDoc = new SolrInputDocument();
    SolrInputDocument newDoc = new SolrInputDocument();

    oldDoc.setField("metric1", "kept-from-old1");
    oldDoc.setField("metric2", "kept-from-old2");
    oldDoc.setField("metric3", "not-kept-from-old");

    assertThat(UpsertCondition.shouldInsertOrUpsert(conditions, oldDoc, newDoc), is(true));
    assertThat(newDoc.getFieldValue("metric1"), is("kept-from-old1"));
    assertThat(newDoc.getFieldValue("metric2"), is("kept-from-old2"));
    assertThat(newDoc.getFieldValue("metric3"), nullValue());
  }

  @Test
  public void givenExistingSoftDeleteAndMetrics_whenCheckingShouldInsertOrUpsert() {
    List<UpsertCondition> conditions = givenMultipleConditions();

    SolrInputDocument oldDoc = new SolrInputDocument();
    SolrInputDocument newDoc = new SolrInputDocument();

    oldDoc.setField("compliance_reason", "soft_delete");
    oldDoc.setField("metric1", "kept-from-old1");
    oldDoc.setField("metric2", "kept-from-old2");
    oldDoc.setField("metric3", "not-kept-from-old");
    newDoc.setField("new_field", "kept-from-new");

    assertThat(UpsertCondition.shouldInsertOrUpsert(conditions, oldDoc, newDoc), is(true));
    assertThat(newDoc.getFieldValue("compliance_reason"), is("soft_delete"));
    assertThat(newDoc.getFieldValue("metric1"), is("kept-from-old1"));
    assertThat(newDoc.getFieldValue("metric2"), is("kept-from-old2"));
    assertThat(newDoc.getFieldValue("metric3"), nullValue());
  }

  @Test
  public void givenForceInsert_whenCheckingShouldInsertOrUpsert() {
    List<UpsertCondition> conditions = givenMultipleConditions();

    SolrInputDocument oldDoc = new SolrInputDocument();
    SolrInputDocument newDoc = new SolrInputDocument();

    oldDoc.setField("compliance_reason", "delete");
    newDoc.setField("force_insert", "true");

    assertThat(UpsertCondition.shouldInsertOrUpsert(conditions, oldDoc, newDoc), is(true));
    assertThat(newDoc.getFieldValue("force_insert"), is("true"));
    assertThat(newDoc.getFieldValue("compliance_reason"), nullValue());
  }

  @Test
  public void givenSkipIfNotExists_whenCheckingShouldInsertOrUpsert() {
    List<UpsertCondition> conditions = givenMultipleConditions();

    SolrInputDocument newDoc = new SolrInputDocument();

    assertThat(UpsertCondition.shouldInsertOrUpsert(conditions, null, newDoc), is(false));
    assertThat(newDoc.isEmpty(), is(true));

    newDoc.setField("date", "today");

    assertThat(UpsertCondition.shouldInsertOrUpsert(conditions, null, newDoc), is(true));
    assertThat(newDoc.getFieldValue("date"), is("today"));
  }

  @Test
  public void givenOldDocMarkedRedact_whenCheckingShouldInsertOrUpsert() {
    List<UpsertCondition> conditions = givenMultipleConditions();

    SolrInputDocument newDoc = new SolrInputDocument();
    SolrInputDocument oldDoc = new SolrInputDocument();

    oldDoc.setField("sensitive_fields", "redact");
    newDoc.setField("sensitive_field1", "should-be-redacted");
    newDoc.setField("sensitive_field2", "should-be-redacted");
    newDoc.setField("new_field", "kept-from-new");

    assertThat(UpsertCondition.shouldInsertOrUpsert(conditions, oldDoc, newDoc), is(true));

    assertThat(newDoc.getField("sensitive_field1"), notNullValue());
    assertThat(newDoc.getField("sensitive_field1").getValue(), nullValue());
    assertThat(newDoc.getField("sensitive_field2"), notNullValue());
    assertThat(newDoc.getField("sensitive_field2").getValue(), nullValue());
    assertThat(newDoc.getFieldValue("sensitive_fields"), is("redact"));
    assertThat(newDoc.getFieldValue("new_field"), is("kept-from-new"));
  }

  @Test(expected = SolrException.class)
  public void givenInvalidConfig_whenReadingConditions() {
    NamedList<Object> args = new NamedList<>();
    args.add("something", "else");

    UpsertCondition.readConditions(args);
  }

  private List<UpsertCondition> givenMultipleConditions() {
    // this roughly represents some sort of "compliance" scenario
    // where we want to delete and/or redact documents in a variety
    // of ways
    // also some other rules are in here around updating metrics etc
    // just so we can test how things interact
    // this might represent a system where we receive deletes + updates
    // requests for documents, but don't have the full document to hand (elsewhere)
    // or we expect to receive delete + update requests _prior_ to receiving the
    // actual documents (at least sometimes)
    NamedList<?> args = namedList(ImmutableListMultimap.<String, NamedList<String>>builder()
        .put("forceInsert", namedList(ImmutableListMultimap.of(
            "must", "NEW.force_insert:true",
            "action", "insert"
        )))
        .put("existingPermanentDeletes", namedList(ImmutableListMultimap.of(
            "must", "OLD.compliance_reason:delete",
            "action", "skip"
        )))
        // updating metrics will fall through as it has no insert
        .put("existingMetrics", namedList(ImmutableListMultimap.of(
            "should", "OLD.metric1:*",
            "should", "OLD.metric2:*",
            "action", "upsert:metric1,metric2"
        )))
        // should also fall through as we'd want redaction to combine with
        // soft deletes (in case the doc is un-deleted later)
        .put("redaction", namedList(ImmutableListMultimap.of(
            "must", "OLD.sensitive_fields:redact",
            "action", "nullify:sensitive_field1,sensitive_field2",
            "action", "upsert:sensitive_fields"
        )))
        .put("existingSoftDeletes", namedList(ImmutableListMultimap.of(
            "must", "OLD.compliance_reason:soft_delete",
            "action", "upsert:compliance_reason",
            "action", "insert"
        )))
        .put("newSoftDeletes", namedList(ImmutableListMultimap.of(
            "must", "NEW.compliance_reason:soft_delete",
            "action", "upsert:*",
            "action", "insert"
        )))
        .put("skipIfNotExists", namedList(ImmutableListMultimap.of(
            "must_not", "OLD.*",
            "must_not", "NEW.date:*",
            "action", "skip"
        )))
        .build()
    );

    return UpsertCondition.readConditions(args);
  }

  private<T> NamedList<T> namedList(ListMultimap<String, T> values) {
    return new NamedList<>(values.entries().toArray(new Map.Entry[0]));
  }
}
