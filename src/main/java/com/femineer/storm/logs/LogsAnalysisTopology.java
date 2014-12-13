package com.femineer.storm.logs;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;

/**
 * This is an exploration of storm to calculate the same log statistics as the Databricks Reference App - Logs Analysis.
 *
 * % storm jar target/uber-storm-logs-analysis-0.0.1-SNAPSHOT.jar com.femineer.storm.logs.LogsAnalysisTopology
 */
public class LogsAnalysisTopology {

  public static void main(String[] args) throws Exception {

    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout("spout", new TextFileSpout("/Users/vida/Code/reference-apps/logs_analyzer/data/apache.accesslog"), 1);

    builder.setBolt("parse_log_line", new ApacheLogLineParserBolt(), 8).shuffleGrouping("spout");

    builder.setBolt("output_content_size",
        new OutputFieldOfApacheAccessLogBolt(ApacheLogLineParserBolt.FIELD_CONTENT_SIZE), 8)
        .localOrShuffleGrouping("parse_log_line");

    // This will eventually need to merge all to one bolt to output the final stats.
    builder.setBolt("merge_content_size_stats", new ContentSizeStatsBolt(), 8).globalGrouping("output_content_size");

    Config conf = new Config();
    conf.setDebug(true);

    if (args != null && args.length > 0) {
      conf.setNumWorkers(3);

      StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
    }
    else {
      conf.setMaxTaskParallelism(3);

      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("word-count", conf, builder.createTopology());

      Thread.sleep(100000);

      cluster.shutdown();
    }
  }
}