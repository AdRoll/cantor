package com.adroll.cantor;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

import org.junit.Test;

import com.google.common.hash.Hashing;

import com.adroll.cantor.HLLWritable;
import com.adroll.cantor.HLLCounter;

public class TestHLLWritable {

  @Test
  public void test_serialization() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(baos);
    
    HLLCounter hll = new HLLCounter(true);
    hll.put("one", "two", "three");
    HLLWritable hllw = new HLLWritable(hll);
    hllw.write(out);
    
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
    HLLWritable deserialized = new HLLWritable();
    
    deserialized.readFields(in);

    HLLCounter d = deserialized.get();
    assertEquals(HLLCounter.DEFAULT_P, d.getP());
    assertEquals(3L, d.size());
    assertEquals(HLLCounter.DEFAULT_K, d.getK());
    assertTrue(d.isIntersectable());
    assertArrayEquals(hll.getByteArray(), d.getByteArray());
    assertArrayEquals(hll.getMinHash().toArray(), d.getMinHash().toArray());
  }
  
  @Test
  public void test_serialization_non_intersectable() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(baos);
    
    HLLCounter hll = new HLLCounter(false);
    hll.put("one", "two", "three");
    HLLWritable hllw = new HLLWritable(hll);
    hllw.write(out);
    
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
    HLLWritable deserialized = new HLLWritable();
    
    deserialized.readFields(in);

    HLLCounter d = deserialized.get();
    assertEquals(HLLCounter.DEFAULT_P, d.getP());
    assertEquals(3L, d.size());
    assertEquals(0, d.getK());
    assertFalse(d.isIntersectable());
    assertNull(d.getMinHash());
    assertArrayEquals(hll.getByteArray(), d.getByteArray());
  }
  
  @Test
  public void test_serialization_larger_than_ts() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(baos);
    
    HLLCounter hll = new HLLCounter(true, 3);
    
    hll.put("one", "two", "three", "four", "five");
    HLLWritable hllw = new HLLWritable(hll);
    hllw.write(out);
    
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
    HLLWritable deserialized = new HLLWritable();
    
    deserialized.readFields(in);

    HLLCounter d = deserialized.get();
    assertEquals(HLLCounter.DEFAULT_P, d.getP());
    assertEquals(5L, d.size());
    assertEquals(3, d.getK());
    assertTrue(d.isIntersectable());
    assertArrayEquals(hll.getByteArray(), d.getByteArray());
    assertArrayEquals(hll.getMinHash().toArray(), d.getMinHash().toArray());
  }
  
  @Test
  public void test_serialization_M_created() throws Exception {
    /**
     * We need to test that M is properly created.
     * set p to 9 so it's different from the default of 18.
     * k needs to be be bigger than the number of elements so -p is written.
     * 
     * Make sure M is recreated when the -p is read in.
     */
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(baos);

    HLLCounter hll = new HLLCounter((byte)9, true, 256); 
    
    hll.put("one", "two", "three", "four", "five");
    HLLWritable hllw = new HLLWritable(hll);
    hllw.write(out);
    
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
    HLLWritable deserialized = new HLLWritable();
    
    deserialized.readFields(in);
    
    HLLCounter d = deserialized.get();
    assertEquals((byte)9, d.getP());
    assertEquals(5L, d.size());
    assertEquals(256, d.getK());
    assertTrue(d.isIntersectable());
    assertArrayEquals(hll.getByteArray(), d.getByteArray());
    assertArrayEquals(hll.getMinHash().toArray(), d.getMinHash().toArray());
  }
    
  @Test
  public void test_intersection() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(baos);
    
    HLLCounter hll0 = new HLLCounter(true);
    for (int i=0; i<1000000; i++) {
      hll0.put(Hashing.murmur3_128().hashString(Integer.toString(i), java.nio.charset.StandardCharsets.UTF_8).toString());
    }

    HLLCounter hll1 = new HLLCounter(true);
    for (int i=10000; i<1100000; i++) {
      hll1.put(Hashing.murmur3_128().hashString(Integer.toString(i), java.nio.charset.StandardCharsets.UTF_8).toString());
    }
    HLLWritable hllw = new HLLWritable(hll0);
    hllw.write(out);
    
    hllw.set(hll1);
    hllw.write(out);
    
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
    HLLWritable deser0 = new HLLWritable();
    deser0.readFields(in);
    
    HLLWritable deser1 = new HLLWritable();
    deser1.readFields(in);
    
    assertEquals(992228, HLLCounter.intersect(deser0.get(), deser1.get()));
  }
  
  @Test
  public void test_set() throws Exception {
    HLLCounter h0 = new HLLCounter();
    HLLCounter h1 = new HLLCounter();
    h0.put("0", "1", "2");
    h1.put("0", "1", "2", "3");

    HLLWritable hllw = new HLLWritable(h0);
    hllw.set(h1);
    assertEquals(hllw.get().size(), 4L);
  }
  
  @Test
  public void test_combination() throws Exception {
    
    HLLCounter h1 = new HLLCounter();
    h1.put("a");
    h1.put("b");
    h1.put("c");

    HLLCounter h2 = new HLLCounter();
    h2.put("d");
    h2.put("e");
    h2.put("f");

    HLLCounter h3 = new HLLCounter();
    h3.put("d");
    h3.put("e");
    h3.put("f");

    HLLWritable w1 = new HLLWritable(h1);
    HLLWritable w2 = new HLLWritable(h2);
    HLLWritable w3 = new HLLWritable(h3);
    
    h1 = w1.combine(w2).get();
    assertTrue(h1.size() == 6L);

    w1 = new HLLWritable(h1);
    h1 = w1.combine(w3).get();
    assertTrue(h1.size() == 6L);

    h2 = w2.combine(w3).get();
    assertTrue(h2.size() == 3L);

    h1.clear();
    h2.clear();
    h3.clear();
    for(int i = 0; i < 1000000; i++) {
      String s = String.valueOf(Math.random());
      h1.put(s);
      h3.put(s);
      if(i > 500000) {
        h2.put(s);
      }
    }

    for(int i = 0; i < 1000000; i++) {
      String s = String.valueOf(Math.random());
      h2.put(s);
      h3.put(s);
    }
    w1 = new HLLWritable(h1);
    w2 = new HLLWritable(h2);
    h1 = w1.combine(w2).get();
    assertTrue(h3.size() == h1.size());
  }
  
  @Test
  public void test_combine_empty() throws Exception {
    HLLWritable empty =  
      new HLLWritable((byte)15, Integer.MAX_VALUE, 0, new byte[(int)Math.pow(2, (byte)15)], new long[0]);
    assertEquals(0, empty.get().getMinHash().size());
    
    HLLWritable empty2 =
      new HLLWritable((byte)15, 8192, 0, new byte[(int)Math.pow(2, (byte)15)], new long[0]);
    assertEquals(0, empty2.get().getMinHash().size());
    
    empty = empty.combine(empty2);
    assertEquals(0, empty.get().getMinHash().size());
    assertEquals(0, empty.get().size());
  }
}
