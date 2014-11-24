package com.adroll.cantor;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.TreeSet;

import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.adroll.cantor.HLLCounter;

/**
   <code>HLLWritable</code> allows for serialization and 
   deserialization of {@link HLLCounter} objects in a 
   Hadoop framework.
*/
public class HLLWritable implements Writable {
  
  private static final Logger LOG = LoggerFactory.getLogger(HLLWritable.class);

  /** The HLL precision of the contained <code>HLLCounter</code> represenation. 
      {@link HLLCounter#MIN_P}<code> &lt;= p &lt;= </code>{@link HLLCounter#MAX_P}.
  */
  protected byte p; 
  /** The MinHash precision of the contained <code>HLLCounter</code> representation. */
  protected int k;
  /** The number of current elements in the MinHash structure 
      of the contained <code>HLLCounter</code> representation. */
  protected int s;
  /** The HLL structure of the contained <code>HLLCounter</code> representation. */
  protected byte[] M;
  /** The contents of the MinHash structure of the contained 
      <code>HLLCounter</code> representation.*/
  protected long[] minhash;

  /**
     Constructs an <code>HLLWritable</code> that contains a representation 
     of the default <code>HLLCounter</code> constructed by 
     {@link HLLCounter#HLLCounter()}.
   */
  public HLLWritable() {
    set(new HLLCounter());
  }

  /**
     Constructs an <code>HLLWritable</code> that contains a representation 
     of the provided <code>HLLCounter</code>.

     @param h the <code>HLLCounter</code> to represent and contain
  */
  public HLLWritable(HLLCounter h) {
    set(h);
  }

  /**
     Constructs an <code>HLLWritable</code> with the given set of fields.
     
     @param p         the <code>byte</code> precision of the HLL 
                      structure. {@link HLLCounter#MIN_P}<code> &lt;= p 
                      &lt;= </code>{@link HLLCounter#MAX_P}.
     @param k         the <code>int</code> precision of the MinHash 
                      structure
     @param s         the <code>int</code> number of elements in the 
                      MinHash structure
     @param M         the <code>byte[]</code> HLL structure
     @param minhash   the <code>long[]</code> elements in the MinHash 
                      structure
  */
  public HLLWritable(byte p, int k, int s, byte[] M, long[] minhash){
    this.p = p;
    this.k = k;
    this.s = s;
    this.M = M;
    this.minhash = minhash;
  }

  /**
     Encapsulates a representation of the given <code>HLLCounter</code>
     in this <code>HLLWritable</code>.

     @param h the <code>HLLCounter</code> to represent and contain
  */
  public void set(HLLCounter h) {
    p = h.getP();
    M = h.getByteArray();
    k = h.getK();
    if(h.isIntersectable()){
      s = h.getMinHash().size();
    } else {
      s = 0;
    }
    if(minhash == null || minhash.length != s){
      minhash = new long[s];
    }
    int i = 0;
    if(h.getMinHash() != null){
      for(Long l : h.getMinHash()){
        minhash[i] = l;
        i++;
      }
    }
  }

  /**
     Returns a new <code>HLLCounter</code> that is constructed 
     from the internal representation of the <code>HLLCounter</code> 
     that this <code>HLLWritable</code> contains.

     @return the <code>HLLCounter</code> this <code>HLLWritable</code>
             represents.
  */
  public HLLCounter get() {
    TreeSet<Long> ts = new TreeSet<Long>();
    for(long l : minhash){
      ts.add(l);
    }
    HLLCounter hll = new HLLCounter(p, k > 0, k, M, ts);
    return hll; 
  }

  /**
     Returns a new <code>HLLWritable</code> that contains a 
     representation of combining its internal 
     <code>HLLCounter</code>'s  representation with
     the other's.
     <p>
     It is functionally equivalent to combining two 
     <code>HLLCounter</code>s 
     ({@link HLLCounter#combine(HLLCounter h)}) and creating a
     new <code>HLLWritable</code> out of that.
     <p>
     Returns <code>null</code> if the combination fails.
     
     @param   other the <code>HLLWritable</code> to combine
     @return  the <code>HLLWritable</code> that represents
              the union, <code>null</code> if fails,
              <code>this</code> if <code>other</code>
              is <code>null</code>.
  */
  public HLLWritable combine(HLLWritable other){
    if(other == null){
      return this;
    }

    byte newP = (byte)Math.min(p, other.p);
    int newK = Math.min(k, other.k);
    byte[] newM = HLLCounter.safeUnion(M, other.M);
    // newMinhash will hold at most newK elements, but possibly less
    long[] newMinhash = new long[newK];
    int i=0, j=0;
    int newS=0;

    try {
      if(newK > 0){
        while ( i < s && j < other.s && newS < newK){
          long left = minhash[i];
          long right = other.minhash[j];
          if(left < right){
            newMinhash[newS] = left;
            i++;
          } else if(left > right){
            newMinhash[newS] = right;
            j++;
          } else { // left == right
            newMinhash[newS] = left;
            i++;
            j++;
          }
          newS++;
        }
        while( i < s && newS < newK){
          newMinhash[newS] = minhash[i];
          i++;
          newS++;
        }
        while(j < other.s && newS < newK){
          newMinhash[newS] = other.minhash[j];
          j++;
          newS++;
        }
        // We allocated an array of newK size, but it's possible we didn't fill it up.
        // This would leave trailing 0's at the end of the array which we don't want to keep around.
        if (newS < newK) {
          newMinhash = Arrays.copyOf(newMinhash, newS);
        }
      }
      return new HLLWritable(newP, newK, newS, newM, newMinhash);
    } catch (Exception e){
      LOG.error("Failed combining", e);
      return null;
    }
  }

  // WritableComparable
  /**
     Serializes this <code>HLLWritable</code> to the given 
     {@link java.io.DataOutput}.
     <p>
     Generally, this method should not be called on its own.

     @param out the <code>DataOutput</code> object to write to
  */
  public void write(DataOutput out) throws IOException {
    try{
      // minhash is not maxed out, M is redundant so don't write it
      if (s < k) {
        // Use -p to signify no M
        out.writeByte(-p);
        out.writeInt(k);
        out.writeInt(s);
        for(int i=0; i < s; i++){
          out.writeLong(minhash[i]);
        }
      } else {
        out.writeByte(p);
        out.writeInt(k);
        out.writeInt(s);
        for(byte b : M){
          out.writeByte(b);
        }
        for(int i=0; i < s; i++){
          out.writeLong(minhash[i]);
        }
      }
    } catch(Exception e){
      LOG.warn("Failed writing", e);
    }
  }

  /**
     Deserialize the fields of this <code>HLLWritable</code> 
     from the given {@link java.io.DataInput}.
     <p>
     Generally, this method should not be called on its own.
     For efficiency, implementations should attempt to re-use
     storage in the existing object where possible.
     
     @param in the <code>DataInput</code> to read from
  */
  public void readFields(DataInput in) throws IOException {
    try {
      p = in.readByte(); 
      k = in.readInt();
      s = in.readInt();
      if(k == 0) {
        s = 0;
      }
      // If p is negative, M does not exist
      if (p < 0) {
        p = (byte) -p;
        int m = (int)Math.pow(2, p);
        M = new byte[m];
      } else {
        int m = (int)Math.pow(2, p);
        M = new byte[m];
        for(int i = 0; i < m; i++) {
          M[i] = in.readByte();
        }
      }
      minhash = new long[s];

      for(int i = 0; i < s; i++) {
        long x = in.readLong();
        minhash[i] = x;
        /**
         * If p was negative, M is empty and we need to re-populate
         * If p was positive and we read M, this won't change anything since it's just max
         */
        int idx = (int)(x >>> (64 - p));
        long w = x << p;
        M[idx] =  (byte)Math.max(M[idx], Long.numberOfLeadingZeros(w) + 1);
      }
    } catch(Exception e) {
      throw new IOException(e);
    }
  }

  /**
     Hashes this <code>HLLWritable</code> based on its
     internal structures.

     @return the <code>int</code> hash value
  */
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + Arrays.hashCode(M);
    result = prime * result + k;
    result = prime * result + Arrays.hashCode(minhash);
    result = prime * result + p;
    result = prime * result + s;
    return result;
  }

  /**
     Returns whether this <code>HLLWritable</code> 
     is equivalent to the given <code>Object</code>.
     <p>
     If the input is another <code>HLLWritable</code>, 
     the two are considered equivalent if all of their 
     fields are equivalent (that is, the two 
     <code>HLLCounters</code> likely saw the exact same
     data).
     
     @param obj the <code>Object</code> to compare to
     
     @return the <code>boolean</code> of the comparison
  */
  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    HLLWritable other = (HLLWritable) obj;
    if (!Arrays.equals(M, other.M)) {
      return false;
    }
    if (k != other.k) {
      return false;
    }
    if (!Arrays.equals(minhash, other.minhash)) {
      return false;
    }
    if (p != other.p) {
      return false;
    }
    if (s != other.s) {
      return false;
    }
    return true;
  }

  /**
     Returns a <code>String</code> representation of this 
     <code>HLLWritable</code>.
     <p>
     The <code>String</code> encodes the <code>p</code>,
     <code>k</code>, and <code>s</code> fields.

     @return the <code>String</code> representation
  */
  @Override
  public String toString() {
    return "HLLWritable [p=" + p + ", k=" + k + ", s=" + s + "]";
  }
}
