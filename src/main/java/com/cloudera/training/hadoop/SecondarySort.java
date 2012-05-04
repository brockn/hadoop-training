package com.cloudera.training.hadoop;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.cloudera.training.hadoop.io.TextIntPairWritable;

public class SecondarySort extends Configured implements Tool {
  public static class InnerMapper extends MapReduceBase
  implements Mapper<Text,Text,TextIntPairWritable,IntWritable> {
    private final IntWritable outputvalue = new IntWritable();
    @Override
    public void map(Text key, Text value,
        OutputCollector<TextIntPairWritable,IntWritable> output,
        Reporter report) throws IOException {
      outputvalue.set(Integer.parseInt(value.toString().trim()));
      TextIntPairWritable pair = new TextIntPairWritable(key, outputvalue);
      output.collect(pair, outputvalue);
    }
  }
  public static class InnerReducer extends MapReduceBase
  implements Reducer<TextIntPairWritable, IntWritable, Text, IntWritable> {
    @Override
    public void reduce(TextIntPairWritable key, Iterator<IntWritable> values,
        OutputCollector<Text, IntWritable> output,
        Reporter reporter) throws IOException {
      while(values.hasNext()) {
        output.collect(key.left, values.next());
      }
    }
  }

  @Override
  public int run(String[] args) throws Exception {
    Path input = new Path("ss/input");
    Path output = new Path("ss/output");
    JobConf job = new JobConf(getConf(), SecondarySort.class);
    job.setNumReduceTasks(1);
    job.setInputFormat(KeyValueTextInputFormat.class);
    job.setPartitionerClass(TextIntPairWritable.LeftPartioner.class);
    job.setOutputValueGroupingComparator(TextIntPairWritable.GroupingOnTextComparator.class);
    job.setMapperClass(InnerMapper.class);
    job.setMapOutputKeyClass(TextIntPairWritable.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setReducerClass(InnerReducer.class);
    FileInputFormat.addInputPath(job, input);
    FileOutputFormat.setOutputPath(job, output);
    JobClient.runJob(job);
    return 0;
  }
  public static void main(String[] args) throws Exception {
    SecondarySort tool = new SecondarySort();
    int rc =  ToolRunner.run(tool, args);
    System.exit(rc);
  }

}
