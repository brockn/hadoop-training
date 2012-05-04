package com.cloudera.training.hadoop.sssp2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class SSSPMapper extends MapReduceBase
implements Mapper<IntWritable, Node, IntWritable, Node> {

  private final Node n = new Node();
  private final int[] neighb = {};

  @Override
  public void map(IntWritable key,
      Node value,
      OutputCollector<IntWritable, Node> output,
      Reporter reporter)
          throws IOException {

    // xxx Consume input.  For output(s):
    // xxx    output.collect(xxxOutputKey, xxxOutputValue);

    int distance;
    output.collect(key, value);
    for (int i=0; i<value.neighbours.length; i++) {
      n.set(neighb, value.shortest+1);
      output.collect(new IntWritable(value.neighbours[i]), n);
    }
  }
}
