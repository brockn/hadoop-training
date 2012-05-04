package com.cloudera.training.hadoop.sssp2;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SSSP extends Configured implements Tool {

  @Override
  public int run(String[] args) throws Exception {

    if (args.length != 2) {
      System.out.printf(
          "Usage: %s [generic options] <indir> <output dir>\n",
          getClass().getSimpleName());
      ToolRunner.printGenericCommandUsage(System.out);
      System.exit(-1);
    }

    JobConf conf = new JobConf(getConf(), SSSP.class);
    conf.setJobName("SSSP");

    conf.setInputFormat(SequenceFileInputFormat.class);
    conf.setOutputFormat(SequenceFileOutputFormat.class);

    FileInputFormat.setInputPaths(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));

    conf.setMapperClass(SSSPMapper.class);
    conf.setReducerClass(SSSPReducer.class);

    conf.setMapOutputKeyClass(IntWritable.class);
    conf.setMapOutputValueClass(Node.class);

    conf.setOutputKeyClass(IntWritable.class);
    conf.setOutputValueClass(Node.class);

    JobClient.runJob(conf);
    return 0;
  }

  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new SSSP(), args);
    System.exit(exitCode);
  }
}
