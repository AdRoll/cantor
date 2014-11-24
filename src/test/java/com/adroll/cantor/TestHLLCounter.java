package com.adroll.cantor;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Random;

import static org.junit.Assert.*;

import org.junit.Test;

import com.adroll.cantor.HLLCounter;

public class TestHLLCounter {

  @Test
  public void test_serialization() throws IOException, ClassNotFoundException {
    HLLCounter h = new HLLCounter();
    h.put("a");
    h.put("b");
    h.put("c");
    assertTrue(h.size() == 3L);

    File f = new File("hll.ser");
    FileOutputStream fos = new FileOutputStream(f);
    ObjectOutputStream oos = new ObjectOutputStream(fos);
    oos.writeObject(h);
    oos.close();
    
    FileInputStream fis = new FileInputStream(f);
    ObjectInputStream ois = new ObjectInputStream(fis);
    HLLCounter hi = (HLLCounter)ois.readObject();
    assertTrue(hi.size() == 3L);
    hi.put("d");
    assertTrue(hi.size() == 4L);

    assertTrue(f.delete());
  }

  @Test
  public void test_combination() throws Exception {
    HLLCounter h1 = new HLLCounter();
    h1.put("a");
    h1.put("b");
    h1.put("c");
    assertTrue(h1.size() == 3L);

    HLLCounter h2 = new HLLCounter();
    h2.put("d");
    h2.put("e");
    h2.put("f");
    assertTrue(h2.size() == 3L);

    HLLCounter h3 = new HLLCounter();
    h3.put("d");
    h3.put("e");
    h3.put("f");
    assertTrue(h3.size() == 3L);

    h1.combine(h2);
    assertTrue(h1.size() == 6L);

    h1.combine(h3);
    assertTrue(h1.size() == 6L);

    h2.combine(h3);
    assertTrue(h2.size() == 3L);

    h1.clear();
    h2.clear();
    h3.clear();

    //h1 and h3 are the same, h2 is a subset
    for(int i = 0; i < 1000000; i++) {
      String s = String.valueOf(Math.random());
      h1.put(s);
      h3.put(s);
      if(i > 500000) {
        h2.put(s);
      }
    }

    //Add more uniques to h2, same to h3
    for(int i = 0; i < 1000000; i++) {
      String s = String.valueOf(Math.random());
      h2.put(s);
      h3.put(s);
    }
    
    //So now the union of h1 and h2 should
    //be h3.
    h1.combine(h2);
    assertTrue(h3.size() == h1.size());
  }

	@Test
	public void test_basic() {
    Random r = new Random(4618201L);
    HLLCounter h = new HLLCounter((byte)8);
    fillHLLCounter(h, r, 25851093);
    assertTrue(h.size() == 22787413L);

    r = new Random(8315542L); 
    h = new HLLCounter((byte)9); 
    fillHLLCounter(h, r, 4954434); 
    assertTrue(h.size() == 5013953L);

    //default precision of HLLCounter.DEFAULT_P = 18
    h = new HLLCounter();
    r = new Random(73919566L); 
    fillHLLCounter(h, r, 17078033); 
    assertTrue(h.size() == 17034653L);

    h.clear();
    r = new Random(57189216L); 
    fillHLLCounter(h, r, 18592874); 
    assertTrue(h.size() == 18526241L);

    h.clear();
    r = new Random(10821894L);
    fillHLLCounter(h, r, 3777716); 
    assertTrue(h.size() == 3760602L);
	}

  @Test
  public void test_fold() {
    Random r = new Random(123456L);
    HLLCounter small = new HLLCounter((byte)8);
    fillHLLCounter(small, r, 100000);

    r = new Random(123456L);
    HLLCounter big = new HLLCounter((byte)12);
    fillHLLCounter(big, r, 100000);
    big.fold((byte)8);
    
    assertEquals(big.size(), small.size());

    r = new Random(23456L);
    small = new HLLCounter((byte)4);
    fillHLLCounter(small, r, 100000);

    r = new Random(23456L);
    big = new HLLCounter((byte)18);
    fillHLLCounter(big, r, 100000);
    big.fold((byte)4);
    
    assertEquals(big.size(), small.size());

    r = new Random(3456L);
    small = new HLLCounter((byte)7);
    fillHLLCounter(small, r, 100000);

    r = new Random(3456L);
    big = new HLLCounter((byte)16);
    fillHLLCounter(big, r, 100000);
    big.fold((byte)7);
    
    assertEquals(big.size(), small.size());
  }

  @Test
  public void test_intersection() {
    HLLCounter h0 = new HLLCounter(true, 1024);
    HLLCounter h1 = new HLLCounter(true, 1024);
    HLLCounter h2 = new HLLCounter(true, 1024);
    HLLCounter h3 = new HLLCounter(true, 1024);
    for(int i = 0; i < 10000; i++) {
      h0.put(String.valueOf(i));
    }
    for(int i = 5000; i < 15000; i++) {
      h1.put(String.valueOf(i));
    }
    for(int i = 8000; i < 11000; i++) {
      h2.put(String.valueOf(i));
    }
    for(int i = 8000; i < 9000; i++) {
      h3.put(String.valueOf(i));
    }

    assertEquals(4853, HLLCounter.intersect(h0, h1)); //about 5000
    assertEquals(1922, HLLCounter.intersect(h0, h2)); //about 2000
    assertEquals(937, HLLCounter.intersect(h0, h3)); //about 1000
    assertEquals(2963, HLLCounter.intersect(h1, h2)); //about 3000
    assertEquals(958, HLLCounter.intersect(h1, h3)); //about 1000
    assertEquals(986, HLLCounter.intersect(h2, h3)); //about 1000
    assertEquals(1862, HLLCounter.intersect(h0, h1, h2)); //about 2000
    assertEquals(762, HLLCounter.intersect(h0, h1, h3)); //about 1000
    assertEquals(934, HLLCounter.intersect(h0, h2, h3)); //about 1000
    assertEquals(958, HLLCounter.intersect(h1, h2, h3)); //about 1000
    assertEquals(762, HLLCounter.intersect(h0, h1, h2, h3)); //about 1000
    assertEquals(0, HLLCounter.intersect());
    assertEquals(0, HLLCounter.intersect(new HLLCounter(), h0));    
    
  }

  private void fillHLLCounter(HLLCounter h, Random r, int n) {
    for(int i = 0; i < n; i++) {
      h.put(String.valueOf(r.nextDouble()));
    }
  }
}
