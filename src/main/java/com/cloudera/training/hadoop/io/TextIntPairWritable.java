package com.cloudera.training.hadoop.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;

public class TextIntPairWritable implements WritableComparable<TextIntPairWritable> {
  public Text left = new Text();
  public IntWritable right = new IntWritable();
  /* empty constructor required for serialization */
  public TextIntPairWritable() {

  }
  public TextIntPairWritable(String left, int right) {
    this.left.set(left);
    this.right.set(right);
  }

  public TextIntPairWritable(Text left, IntWritable right) {
    this.left.set(left.getBytes());
    this.right.set(right.get());
  }
  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(left.toString());
    out.writeInt(right.get());
  }
  @Override
  public void readFields(DataInput in) throws IOException {
    String s = in.readUTF();
    left.set(s);
    int i = in.readInt();
    right.set(i);
  }
  /*
   * This method will be used when sorting. We want to sort
   * by the natural key first "left" and then by our value "right"
   */
  @Override
  public int compareTo(TextIntPairWritable other) {
    int ret = left.compareTo(other.left);
    if(ret == 0) {
      return right.compareTo(other.right);
    }
    return ret;
  }
  @Override
  public boolean equals(Object o) {
    TextIntPairWritable other = (TextIntPairWritable)o;
    return left.equals(other.left) && right.equals(other.right);
  }
  @Override
  public int hashCode() {
    return left.hashCode();
  }
  @Override
  public String toString() {
    return "(" + left + "," + right + ")";
  }

  public static class LeftPartioner implements Partitioner<TextIntPairWritable, Object> {
    @Override
    public int getPartition(TextIntPairWritable key, Object value, int numPartitions) {
      return key.left.hashCode() & Integer.MAX_VALUE % numPartitions;
    }
    @Override
    public void configure(JobConf job) {
    }
  }

  public static class GroupingOnTextComparator implements RawComparator<TextIntPairWritable> {
    DataInputBuffer buffer = new DataInputBuffer();
    TextIntPairWritable key1 = new TextIntPairWritable();
    TextIntPairWritable key2 = new TextIntPairWritable();
    @Override
    public int compare(TextIntPairWritable key1, TextIntPairWritable key2) {
      return key1.left.compareTo(key2.left);
    }
    // TODO to the actual binary comparison as opposed to this hacked one
    @Override
    public int compare(byte[] bytes1, int start1, int len1, byte[] bytes2, int start2, int len2) {
      try {
        buffer.reset(bytes1, start1, len1);
        key1.readFields(buffer);
        buffer.reset(bytes2, start2, len2);
        key2.readFields(buffer);
        return compare(key1, key2);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
