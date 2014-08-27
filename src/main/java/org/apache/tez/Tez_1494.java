package org.apache.tez;

import com.google.common.base.Preconditions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.tez.client.TezClient;
import org.apache.tez.common.TezCommonUtils;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.DataSinkDescriptor;
import org.apache.tez.dag.api.DataSourceDescriptor;
import org.apache.tez.dag.api.Edge;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.dag.api.client.Progress;
import org.apache.tez.dag.api.client.StatusGetOpts;
import org.apache.tez.mapreduce.input.MRInput;
import org.apache.tez.mapreduce.output.MROutput;
import org.apache.tez.mapreduce.processor.SimpleMRProcessor;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.api.Reader;
import org.apache.tez.runtime.api.Writer;
import org.apache.tez.runtime.library.api.KeyValueReader;
import org.apache.tez.runtime.library.api.KeyValueWriter;
import org.apache.tez.runtime.library.api.KeyValuesReader;
import org.apache.tez.runtime.library.conf.OrderedPartitionedKVEdgeConfig;
import org.apache.tez.runtime.library.conf.UnorderedKVEdgeConfig;
import org.apache.tez.runtime.library.partitioner.HashPartitioner;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Simple test case for reproducing TEZ-1494 issue
 *
 * Create 2 sample text files and upload to HDFS
 * Used the following command to run the test.  Use -Dtez.shuffle-vertex-manager.enable
 * .auto-parallel=true to reproduce the issue
 * HADOOP_USER_CLASSPATH_FIRST=true HADOOP_CLASSPATH=/root/tez-1494/tez_1494.jar:/root/tez-autobuild/dist/tez/*:/root/tez-autobuild/dist/tez/lib/*:/root/tez-autobuild/dist/tez/conf:$HADOOP_CLASSPATH yarn jar /root/tez-1494/tez_1494.jar org.apache.tez.Tez_1494 -Dtez.shuffle-vertex-manager.enable.auto-parallel=true /tmp/map_2.out /tmp/map_7.out /tmp/test.out.1484
 */
public class Tez_1494 extends Configured implements Tool {

  static final String PROCESSOR_NAME = "processorName";
  static final String SLEEP_INTERVAL = "sleepInterval";

  public static class DummyProcessor extends SimpleMRProcessor {

    private static final Log LOG = LogFactory.getLog(DummyProcessor.class);
    Configuration conf;
    String processorName;
    String logId;
    long sleepInterval;

    public DummyProcessor(ProcessorContext context) {
      super(context);
    }

    @Override public void run() throws Exception {
      LOG.info("Emitting zero records : " + getContext().getTaskIndex() + " : taskVertexIndex:" +
          getContext().getTaskVertexIndex() + "; " + logId);
      if (sleepInterval > 0) {
        LOG.info("Sleeping for " + sleepInterval + "; " + logId);
        Thread.sleep(sleepInterval);
      }
    }

    @Override public void initialize() throws Exception {
      super.initialize();
      if (getContext().getUserPayload() != null) {
        conf = TezUtils.createConfFromUserPayload(getContext().getUserPayload());
        this.processorName = conf.get(PROCESSOR_NAME);
        this.logId = this.processorName;
        this.sleepInterval = conf.getLong(SLEEP_INTERVAL, 0);
      }
    }
  }

  /**
   * Echo processor with single output writer (Good enough for this test)
   */
  public static class EchoProcessor extends SimpleMRProcessor {

    private static final Log LOG = LogFactory.getLog(EchoProcessor.class);

    private String processorName;
    private String logId;

    public EchoProcessor(ProcessorContext context) {
      super(context);
    }

    @Override public void initialize() throws Exception {
      super.initialize();
      if (getContext().getUserPayload() != null) {
        Configuration conf = TezUtils.createConfFromUserPayload(getContext().getUserPayload());
        this.processorName = conf.get(PROCESSOR_NAME);
        this.logId = this.processorName;
      }
    }

    @Override public void run() throws Exception {
      LOG.info(processorName + " Inputs : " + getInputs().values() + "; outputs: " + getOutputs()
          .values());
      Writer writer = getOutputs().values().iterator().next().getWriter();

      Iterator<LogicalInput> inputIterator = getInputs().values().iterator();
      while (inputIterator.hasNext()) {
        Reader reader = inputIterator.next().getReader();
        //TEZ-1503
        if (reader instanceof KeyValueReader) {
          copy((KeyValueReader) reader, (KeyValueWriter) writer);
        }
        if (reader instanceof KeyValuesReader) {
          copy((KeyValuesReader) reader, (KeyValueWriter) writer);
        }
      }
    }
  }

  static void copy(KeyValuesReader reader, KeyValueWriter writer) throws IOException {
    while (reader.next()) {
      for (Object val : reader.getCurrentValues()) {
        writer.write(reader.getCurrentKey(), val);
      }
    }
  }

  static void copy(KeyValueReader reader, KeyValueWriter writer) throws IOException {
    while (reader.next()) {
      writer.write(reader.getCurrentKey(), reader.getCurrentValue());
    }
  }

  public static Path getCurrentJarURL() throws URISyntaxException {
    return new Path(Tez_1494.class.getProtectionDomain().getCodeSource()
        .getLocation().toURI());
  }

  private Map<String, LocalResource> getLocalResources(TezConfiguration tezConf) throws
      IOException, URISyntaxException {
    Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
    Path stagingDir = TezCommonUtils.getTezBaseStagingPath(tezConf);

    // staging dir
    FileSystem fs = FileSystem.get(tezConf);
    Path jobJar = new Path(stagingDir, "job.jar");
    if (fs.exists(jobJar)) {
      fs.delete(jobJar, false);
    }
    fs.copyFromLocalFile(getCurrentJarURL(), jobJar);

    localResources.put("job.jar", createLocalResource(fs, jobJar));
    return localResources;
  }

  private LocalResource createLocalResource(FileSystem fs, Path file) throws IOException {
    final LocalResourceType type = LocalResourceType.FILE;
    final LocalResourceVisibility visibility = LocalResourceVisibility.APPLICATION;
    FileStatus fstat = fs.getFileStatus(file);
    org.apache.hadoop.yarn.api.records.URL resourceURL = ConverterUtils.getYarnUrlFromPath(file);
    long resourceSize = fstat.getLen();
    long resourceModificationTime = fstat.getModificationTime();
    LocalResource lr = Records.newRecord(LocalResource.class);
    lr.setResource(resourceURL);
    lr.setType(type);
    lr.setSize(resourceSize);
    lr.setVisibility(visibility);
    lr.setTimestamp(resourceModificationTime);
    return lr;
  }

  private ProcessorDescriptor createDescriptor(String processorClassName)
      throws IOException {
    ProcessorDescriptor descriptor = ProcessorDescriptor.create(processorClassName);

    UserPayload payload = TezUtils.createUserPayloadFromConf(getConf());
    //not the best approach to send the entire config in payload.  But for testing, its fine
    descriptor.setUserPayload(payload);
    return descriptor;
  }

  private Vertex createVertex(String vName, String pName, String pClassName, String dataSource,
      DataSourceDescriptor dsource, String dataSink, DataSinkDescriptor dsink, int parallelism,
      long sleepInterval
  ) throws IOException {
    parallelism = (parallelism <= 0) ? -1 : parallelism;
    sleepInterval = (sleepInterval <= 0) ? 0 : sleepInterval;
    getConf().set(PROCESSOR_NAME, pName);
    getConf().setLong(SLEEP_INTERVAL, sleepInterval);
    Vertex v = Vertex.create(vName, createDescriptor(pClassName), parallelism);
    if (dataSource != null) {
      Preconditions.checkArgument(dsource != null, "datasource can't be null");
      v.addDataSource(dataSource, dsource);
    }

    if (dataSink != null) {
      Preconditions.checkArgument(dsink != null, "datasink can't be null");
      v.addDataSink(dataSink, dsink);
    }
    return v;
  }

  public DAG createDAG(String map_2_input, String map_7_input, String output,
      TezConfiguration tezConf) throws IOException, URISyntaxException {

    List<Vertex> vertexList = new ArrayList<Vertex>();
    List<Edge> edgeList = new ArrayList<Edge>();

    /**
     * source1 --> Map_2 --(Scatter_Gather)--> Reducer_3
     * source2 --> Map_7 ----(unordered)---\\
     *                                       ==> Map_5
     * Reducer_3 -------(unordered)--------//
     * Map_5 --(Scatter_Gather)---> Reducer_6
     * Reducer_6 --> Sink
     */
    DataSourceDescriptor map_2_dataDescriptor = MRInput.createConfigBuilder(getConf(),
        TextInputFormat.class, map_2_input).build();
    DataSourceDescriptor map_7_dataDescriptor = MRInput.createConfigBuilder(getConf(),
        TextInputFormat.class, map_7_input).build();

    DataSinkDescriptor sinkDescriptor = MROutput.createConfigBuilder(getConf(),
        TextOutputFormat.class, output).build();

    String name = "Map_2";
    Vertex map_2 = createVertex(name, name, DummyProcessor.class.getName(), "Map_2_input",
        map_2_dataDescriptor, null, null, -1, 0);
    vertexList.add(map_2);

    name = "Reducer_3";
    Vertex reducer_3 = createVertex(name, name, DummyProcessor.class.getName(), null, null, null,
        null, 2, 10000);
    vertexList.add(reducer_3);

    name = "Map_5";
    Vertex map_5 = createVertex(name, name, EchoProcessor.class.getName(), null, null, null,
        null, 10, 0);
    vertexList.add(map_5);

    name = "Map_7";
    Vertex map_7 =
        createVertex(name, name, EchoProcessor.class.getName(), "Map_7_input", map_7_dataDescriptor,
            null,
            null, -1, 0);
    vertexList.add(map_7);

    name = "Reducer_6";
    Vertex reducer_6 = createVertex(name, name, EchoProcessor.class.getName(), null,
        null, "Reducer_6_sink", sinkDescriptor, 1, 0);
    vertexList.add(reducer_6);

    Edge m2_r3_edge = Edge.create(map_2, reducer_3, OrderedPartitionedKVEdgeConfig.newBuilder(LongWritable
            .class
            .getName(),
        Text.class.getName(), HashPartitioner.class.getName()).build()
        .createDefaultEdgeProperty());

    Edge m7_m5_edge = Edge.create(map_7, map_5, UnorderedKVEdgeConfig.newBuilder(LongWritable.class
        .getName(), Text.class.getName()).build().createDefaultBroadcastEdgeProperty());

    Edge r3_m5_edge =
        Edge.create(reducer_3, map_5, UnorderedKVEdgeConfig.newBuilder(LongWritable.class
            .getName(), Text.class.getName()).build().createDefaultBroadcastEdgeProperty());

    Edge m5_r6_edge =
        Edge.create(map_5, reducer_6, OrderedPartitionedKVEdgeConfig.newBuilder(LongWritable
            .class.getName(), Text.class.getName(), HashPartitioner.class.getName()).build()
            .createDefaultEdgeProperty());

    edgeList.add(m2_r3_edge);
    edgeList.add(m7_m5_edge);
    edgeList.add(r3_m5_edge);
    edgeList.add(m5_r6_edge);

    DAG dag = DAG.create("TEZ_1494");
    dag.addTaskLocalFiles(getLocalResources(tezConf));
    for (Vertex v : vertexList) {
      dag.addVertex(v);
    }

    for (Edge edge : edgeList) {
      dag.addEdge(edge);
    }

    return dag;
  }

  @Override public int run(String[] args) throws Exception {
    TezConfiguration tezConf = new TezConfiguration(getConf());

    String[] otherArgs = new GenericOptionsParser(args).getRemainingArgs();

    Preconditions.checkArgument(otherArgs != null && otherArgs.length == 3,
        "Please provide valid map_2_input map_7_input reducer_output path");

    String map_2_input = otherArgs[0];
    String map_7_input = otherArgs[1];
    String output = otherArgs[2];

    TezClient tezClient = TezClient.create("Tez_1494", tezConf, false);
    tezClient.start();
    DAG dag = createDAG(map_2_input, map_7_input, output, tezConf);

    try {
      DAGClient statusClient = tezClient.submitDAG(dag);

      //Start monitor
      Monitor monitor = new Monitor(statusClient);

      DAGStatus status = statusClient.waitForCompletionWithStatusUpdates(
          EnumSet.of(StatusGetOpts.GET_COUNTERS));

      return (status.getState() == DAGStatus.State.SUCCEEDED) ? 0 : -1;
    } finally {
      if (tezClient != null) {
        tezClient.stop();
      }
    }
  }

  //Most of this is borrowed from Hive. Trimmed down to fit this test
  static class Monitor extends Thread {
    DAGClient client;

    public Monitor(DAGClient client) {
      this.client = client;
      this.start();
    }
    public void run() {
      try {
        report(client);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    public void report(DAGClient dagClient) throws Exception {

      Set<StatusGetOpts> opts = new HashSet<StatusGetOpts>();
      DAGStatus.State lastState = null;
      String lastReport = null;

      while(true) {
        try {

          DAGStatus status = dagClient.getDAGStatus(opts);
          Map<String, Progress> progressMap = status.getVertexProgress();
          DAGStatus.State state = status.getState();

          if (state != lastState || state == state.RUNNING) {
            lastState = state;

            switch (state) {
            case SUBMITTED:
              System.out.println("Status: Submitted");
              break;
            case INITING:
              System.out.println("Status: Initializing");
              break;
            case RUNNING:
              printStatus(progressMap, lastReport);
              break;
            }
          }
          Thread.sleep(2000);
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }

    private void printStatus(Map<String, Progress> progressMap, String lastReport) {
      StringBuffer reportBuffer = new StringBuffer();

      SortedSet<String> keys = new TreeSet<String>(progressMap.keySet());
      for (String s: keys) {
        Progress progress = progressMap.get(s);
        final int complete = progress.getSucceededTaskCount();
        final int total = progress.getTotalTaskCount();
        final int running = progress.getRunningTaskCount();
        final int failed = progress.getFailedTaskCount();
        if (total <= 0) {
          reportBuffer.append(String.format("%s: -/-\t", s, complete, total));
        } else {
          if(complete < total && (complete > 0 || running > 0 || failed > 0)) {
          /* vertex is started, but not complete */
            if (failed > 0) {
              reportBuffer.append(String.format("%s: %d(+%d,-%d)/%d\t", s, complete, running, failed, total));
            } else {
              reportBuffer.append(String.format("%s: %d(+%d)/%d\t", s, complete, running, total));
            }
          } else {
          /* vertex is waiting for input/slots or complete */
            if (failed > 0) {
            /* tasks finished but some failed */
              reportBuffer.append(String.format("%s: %d(-%d)/%d\t", s, complete, failed, total));
            } else {
              reportBuffer.append(String.format("%s: %d/%d\t", s, complete, total));
            }
          }
        }
      }

      String report = reportBuffer.toString();
      System.out.println(report);
    }
  }



  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(), new Tez_1494(), args);
  }
}

