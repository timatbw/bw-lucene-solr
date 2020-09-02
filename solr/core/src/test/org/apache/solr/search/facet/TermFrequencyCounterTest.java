package org.apache.solr.search.facet;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.common.util.JavaBinCodec;
import org.junit.Test;

public class TermFrequencyCounterTest extends LuceneTestCase {
  private static final char[] ALPHABET = "abcdefghijklkmnopqrstuvwxyz".toCharArray();

  @Test
  public void testAddValue() throws IOException {
    int iters = 10 * RANDOM_MULTIPLIER;

    for (int i = 0; i < iters; i++) {
      TermFrequencyCounter counter = new TermFrequencyCounter();

      int numValues = random().nextInt(100);
      Map<String, Integer> expected = new HashMap<>();
      for (int j = 0; j < numValues; j++) {
        String value = randomString(ALPHABET, random().nextInt(256));
        int count = random().nextInt(256);

        addCount(counter, value, count);

        expected.merge(value, count, Integer::sum);
      }

      expected.forEach((value, count) -> assertCount(counter, value, count));

      TermFrequencyCounter serialized = serdeser(counter, random().nextInt(Integer.MAX_VALUE));

      expected.forEach((value, count) -> assertCount(serialized, value, count));
    }
  }

  @Test
  public void testMerge() throws IOException {
    int iters = 10 * RANDOM_MULTIPLIER;

    for (int i = 0; i < iters; i++) {
      TermFrequencyCounter x = new TermFrequencyCounter();

      int numXValues = random().nextInt(100);
      Map<String, Integer> expectedXValues = new HashMap<>();
      for (int j = 0; j < numXValues; j++) {
        String value = randomString(ALPHABET, random().nextInt(256));
        int count = random().nextInt(256);

        addCount(x, value, count);

        expectedXValues.merge(value, count, Integer::sum);
      }

      expectedXValues.forEach((value, count) -> assertCount(x, value, count));

      TermFrequencyCounter y = new TermFrequencyCounter();

      int numYValues = random().nextInt(100);
      Map<String, Integer> expectedYValues = new HashMap<>();
      for (int j = 0; j < numYValues; j++) {
        String value = randomString(ALPHABET, random().nextInt(256));
        int count = random().nextInt(256);

        addCount(y, value, count);

        expectedYValues.merge(value, count, Integer::sum);
      }

      expectedYValues.forEach((value, count) -> assertCount(y, value, count));

      TermFrequencyCounter merged = merge(x, y, random().nextInt(Integer.MAX_VALUE));

      expectedYValues.forEach((value, count) -> expectedXValues.merge(value, count, Integer::sum));

      expectedXValues.forEach((value, count) -> assertCount(merged, value, count));
    }
  }

  private static String randomString(char[] alphabet, int length) {
    final StringBuilder sb = new StringBuilder(length);
    for (int i = 0; i < length; i++) {
      sb.append(alphabet[random().nextInt(alphabet.length)]);
    }
    return sb.toString();
  }

  private static void addCount(TermFrequencyCounter counter, String value, int count) {
    for (int i = 0; i < count; i++) {
      counter.add(value);
    }
  }

  private static void assertCount(TermFrequencyCounter counter, String value, int count) {
    assertEquals(
      "value " + value + " should have count " + count,
      count,
      (int) counter.getCounters().getOrDefault(value, 0)
    );
  }

  private static TermFrequencyCounter serdeser(TermFrequencyCounter counter, int limit) throws IOException {
    JavaBinCodec codec = new JavaBinCodec();

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    codec.marshal(counter.serialize(limit), out);

    InputStream in = new ByteArrayInputStream(out.toByteArray());
    counter = new TermFrequencyCounter();
    counter.merge((Map<String, Integer>) codec.unmarshal(in));

    return counter;
  }

  private static TermFrequencyCounter merge(
    TermFrequencyCounter counter,
    TermFrequencyCounter toMerge,
    int limit
  ) throws IOException {
    JavaBinCodec codec = new JavaBinCodec();

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    codec.marshal(toMerge.serialize(limit), out);

    InputStream in = new ByteArrayInputStream(out.toByteArray());
    counter.merge((Map<String, Integer>) codec.unmarshal(in));

    return counter;
  }
}
