package org.apache.solr.search.facet;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.function.IntFunction;

import org.apache.lucene.queries.function.ValueSource;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.search.FunctionQParser;
import org.apache.solr.search.SyntaxError;
import org.apache.solr.search.ValueSourceParser;
import org.apache.solr.search.facet.SlotAcc.FuncSlotAcc;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


public class BitmapCollectorAgg extends SimpleAggValueSource {

  private static final String KEY = "bitmap";

  public static class Parser extends ValueSourceParser {
    @Override
    public ValueSource parse(FunctionQParser fp) throws SyntaxError {
      return new BitmapCollectorAgg(fp.parseValueSource());
    }
  }

  public BitmapCollectorAgg(ValueSource vs) {
    super("bitmapcollector", vs);
  }

  @Override
  public SlotAcc createSlotAcc(FacetContext fcontext, int numDocs, int numSlots) {
    return new Acc(getArg(), fcontext, numSlots);
  }

  @Override
  public FacetMerger createFacetMerger(Object prototype) {
    return new Merger();
  }

  @Override
  public String description() {
    return "bitmapcollector";
  }


  private class Acc extends FuncSlotAcc {
    MutableRoaringBitmap[] result;

    Acc(ValueSource vs, FacetContext fcontext, int numSlots) {
      super(vs, fcontext, numSlots);
      this.result = new MutableRoaringBitmap[numSlots];
    }

    @Override
    public void collect(int doc, int slot, IntFunction<SlotContext> slotContext) throws IOException {
      if (result[slot] == null) {
        result[slot] = new MutableRoaringBitmap();
      }
      result[slot].add(values.intVal(doc));
    }

    @Override
    public int compare(int slotA, int slotB) {
      return slotA - slotB;
    }

    @Override
    public Object getValue(int slotNum) {
      byte[] serialised;
      if (result[slotNum] != null) {
        result[slotNum].runOptimize();
        serialised = bitmapToBytes(result[slotNum]);
      } else {
        serialised = new byte[0];
      }
      SimpleOrderedMap map = new SimpleOrderedMap();
      map.add(KEY, serialised);
      return map;
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

    private MutableRoaringBitmap combined = new MutableRoaringBitmap();

    @Override
    public void merge(Object facetResult, Context mcontext) {
      if (facetResult instanceof SimpleOrderedMap) {
        byte[] bitmapBytes = (byte[])((SimpleOrderedMap)facetResult).get(KEY);
        if (bitmapBytes.length != 0) {
          combined.or(new ImmutableRoaringBitmap(ByteBuffer.wrap(bitmapBytes)));
        }
      }
    }

    @Override
    public void finish(Context mcontext) {
      // never called
    }

    @Override
    public Object getMergedResult() {
      combined.runOptimize();
      SimpleOrderedMap map = new SimpleOrderedMap();
      map.add(KEY, bitmapToBytes(combined));
      return map;
    }
  }

  private static byte[] bitmapToBytes(MutableRoaringBitmap bitmap) {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(bos);
    try {
      bitmap.serialize(dos);
      dos.close();
      return bos.toByteArray();
    } catch (IOException ioe) {
      throw new RuntimeException("Failed to serialise RoaringBitmap to bytes", ioe);
    }
  }
}
