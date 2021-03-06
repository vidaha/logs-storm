package com.femineer.storm.logs;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

// NOTE: This current setup doesn't combine.
public class ContentSizeStatsBolt extends BaseBasicBolt {
  public static final String FIELD_COUNT = "content_size_count";
  public static final String FIELD_SUM = "content_size_sum";
  public static final String FIELD_MINIMUM = "content_size_minimum";
  public static final String FIELD_MAXIMUM = "content_size_maximum";

  public Long runningCount = 0L;
  public Long runningSum = 0L;
  public Long runningMin = null;
  public Long runningMax = null;

  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {
    Long contextSize = tuple.getLong(0);

    runningCount += 1;
    runningSum += contextSize;
    if (runningMin == null || contextSize < runningMin) {
      runningMin = contextSize;
    }
    if (runningMax == null || contextSize > runningMax) {
      runningMax = contextSize;
    }

    collector.emit(new Values(runningCount, runningSum, runningMin, runningMax));
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields(FIELD_COUNT, FIELD_SUM, FIELD_MINIMUM, FIELD_MAXIMUM));
  }
}
