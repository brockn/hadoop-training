package com.cloudera.training.hadoop.sssp1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class GraphTraverse extends Configured implements Tool {
  private static Log LOGGER = LogFactory.getLog(GraphTraverse.class);

  public static class InnerTextToNodeMapper extends MapReduceBase implements
  Mapper<Text, Text, Text, Node> {
    @Override
    public void map(Text key, Text value,
        OutputCollector<Text, Node> output, Reporter reporter)
            throws IOException {
      // handle possible bad inputs
      String sKey = key.toString().trim();
      String sValue = value.toString().trim();
      if (sKey.equals("") || sValue.equals("")) {
        return;
      }
      key.set(sKey);
      value.set(sValue);


      String[] fields = value.toString().split(",");
      if (fields.length == 0) {
        throw new IOException("Value must be csv");
      }
      int distance = Integer.parseInt(fields[fields.length - 1]);
      String[] friends = new String[0];
      if (fields.length > 1) {
        friends = Arrays.copyOfRange(fields, 0, fields.length - 1);
      }
      Node node = new Node(distance, friends);
      output.collect(key, node);
      if(LOGGER.isDebugEnabled()) {
        LOGGER.debug("Map1 " + key + "\t" + node);
      }
      for (String friend : node.getFriends()) {
        int dist = Integer.MAX_VALUE;
        if (node.getDistance() != Integer.MAX_VALUE) {
          dist = node.getDistance() + 1;
        }
        Node outputValue = new Node(dist);
        output.collect(new Text(friend), outputValue);
        if(LOGGER.isDebugEnabled()) {
          LOGGER.debug("Map2 " + friend + "\t" + outputValue.toString());
        }
      }
    }
  }

  public static class InnerMapper extends MapReduceBase implements
  Mapper<Text, Node, Text, Node> {
    @Override
    public void map(Text key, Node value,
        OutputCollector<Text, Node> output, Reporter reporter)
            throws IOException {
      if(LOGGER.isDebugEnabled()) {
        LOGGER.debug("Map1 " + key + "\t" + value);
      }
      output.collect(key, value);
      for (String friend : value.getFriends()) {
        int dist = Integer.MAX_VALUE;
        if (value.getDistance() != Integer.MAX_VALUE) {
          dist = value.getDistance() + 1;
        }
        Node node = new Node(dist);
        output.collect(new Text(friend), node);
        if(LOGGER.isDebugEnabled()) {
          LOGGER.debug("Map2 " + friend + "\t" + node);
        }
      }
    }
  }

  public static class InnerReducer extends MapReduceBase implements
  Reducer<Text, Node, Text, Node> {
    @Override
    public void reduce(Text key, Iterator<Node> values,
        OutputCollector<Text, Node> output, Reporter reporter)
            throws IOException {
      int min = Integer.MAX_VALUE;
      Node node = null;
      while (values.hasNext()) {
        Node distance = values.next();
        if(LOGGER.isDebugEnabled()) {
          LOGGER.debug("Reduce1 " + key + "\t" + distance.toString());
        }
        min = Math.min(distance.getDistance(), min);
        if (distance.getFriends() != null) {
          if (node != null) {
            throw new IOException("Node " + node.toString() + " already found for "
                + key.toString() + ": " + distance.toString());
          }
          node = new Node(distance.getDistance(), distance.getFriends());
          if(LOGGER.isDebugEnabled()) {
            LOGGER.debug("Reduce2 " + key + "\t" + node.toString());
          }

        }
      }
      if (min < 0) {
        throw new IOException("min " + min + " less than zero "
            + key.toString());
      }
      if (node == null) {
        throw new IOException("Did not find structure for "
            + key.toString());
      }
      if (min != node.getDistance()) {
        reporter.incrCounter(Counters.NUMBER_OF_SHORTER_PATHS_FOUND, 1);
        node.setDistance(min);
      }
      if(LOGGER.isDebugEnabled()) {
        LOGGER.debug("Reduce3 " + key + "\t" + node.toString());
      }
      output.collect(key, node);
    }
  }
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new GraphTraverse(), args);
    System.exit(res);
  }

  public long getNumOfShorterPathsFound(RunningJob job) throws IOException {
    return job.getCounters().getCounter(Counters.NUMBER_OF_SHORTER_PATHS_FOUND);
  }
  public enum Counters {
    NUMBER_OF_SHORTER_PATHS_FOUND;
  }
  @Override
  public int run(String[] args) throws Exception {
    Path input = new Path(args[0]);
    Path output = new Path(args[1]);
    Path finalOutput = output;
    int iterationCount = 1;
    List<Path> outputPaths = new ArrayList<Path>();
    JobConf conf = new JobConf(getConf(), GraphTraverse.class);
    conf.setJobName("Graph Traverse");
    conf.setMapperClass(InnerTextToNodeMapper.class);
    conf.setMapOutputKeyClass(Text.class);
    conf.setMapOutputValueClass(Node.class);
    conf.setReducerClass(InnerReducer.class);
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Node.class);
    conf.setInputFormat(KeyValueTextInputFormat.class);
    conf.setOutputFormat(SequenceFileOutputFormat.class);
    output = new Path(output.getParent(), output.getName() + "-" + (iterationCount));
    outputPaths.add(output);
    FileInputFormat.setInputPaths(conf, input);
    FileOutputFormat.setOutputPath(conf, output);
    RunningJob job = JobClient.runJob(conf);
    long numShorterPathsFound = getNumOfShorterPathsFound(job);
    while (numShorterPathsFound > 0) {
      input = output;
      output = new Path(output.getParent(), output.getName() + "-" + (iterationCount + 1));
      outputPaths.add(output);
      conf = new JobConf(getConf(), GraphTraverse.class);
      conf.setJobName("Graph Traverse " + iterationCount);
      conf.setMapperClass(InnerMapper.class);
      conf.setMapOutputKeyClass(Text.class);
      conf.setMapOutputValueClass(Node.class);
      conf.setReducerClass(InnerReducer.class);
      conf.setOutputKeyClass(Text.class);
      conf.setOutputValueClass(Node.class);
      conf.setInputFormat(SequenceFileInputFormat.class);
      conf.setOutputFormat(SequenceFileOutputFormat.class);
      FileInputFormat.setInputPaths(conf, input);
      FileOutputFormat.setOutputPath(conf, output);
      job = JobClient.runJob(conf);
      numShorterPathsFound = getNumOfShorterPathsFound(job);
      iterationCount++;
    }
    FileSystem fs = FileSystem.get(conf);
    if(!fs.rename(output, finalOutput)) {
      throw new IOException("Could not rename" + output + " to " + finalOutput);
    }
    for(Path path : outputPaths) {
      if(fs.exists(path)) {
        fs.delete(path, true);
      }
    }
    return 0;
  }
}
