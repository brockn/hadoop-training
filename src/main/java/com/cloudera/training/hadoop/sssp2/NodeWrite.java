package com.cloudera.training.hadoop.sssp2;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;

public class NodeWrite {

  public static void main(String[] args) throws IOException {
    String uri = args[0];
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(URI.create(uri), conf);
    Path path = new Path(uri);
    IntWritable key = new IntWritable();
    Node value = new Node();
    SequenceFile.Writer writer = null;
    try {
      int[] keys = {1,2,3,4,5};
      int[][] values = { {2,3}, {4}, {5}, {5}, {} };
      writer = SequenceFile.createWriter(fs, conf, path, key.getClass(), value.getClass());
      for (int i=0; i<keys.length; i++) {
        key.set(keys[i]);
        if (keys[i]==1) {
          value.set(values[i],0);
        } else {
          value.set(values[i]);
        }
        writer.append(key, value);
      }

    } finally {
      IOUtils.closeStream(writer);
    }
  }
}
