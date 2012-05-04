package com.cloudera.training.hadoop.sssp2;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class SSSPReducer extends MapReduceBase
implements Reducer<IntWritable, Node, IntWritable, Node> {

  private int shortest = 99999;
  private int[] neighbours = {};
  private Node n = new Node();

  @Override
  public void reduce(IntWritable key,
      Iterator<Node> values,
      OutputCollector<IntWritable, Node> output,
      Reporter reporter)
          throws IOException {

    shortest = 99999;
    while (values.hasNext()) {

      // xxx Consume values in term values.next()
      n = values.next();
      if (n.shortest < shortest) {
        shortest = n.shortest;
      }
      if (n.neighbours.length>0) {
        neighbours = n.neighbours;
      }

    }
    n.set(neighbours, shortest);
    output.collect(key, n);

    // xxx For output(s):
    // xxx   output.collect(xxxOutputKey, xxxOutputValue);
  }
}
