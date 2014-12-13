package com.femineer.storm.logs;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * This
 */
public class OutputFieldOfApacheAccessLogBolt extends BaseBasicBolt {
  private String field;
  public OutputFieldOfApacheAccessLogBolt(String field) {
    this.field = field;
  }

  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {
    Long contextSize = tuple.getLongByField(field);
    collector.emit(new Values(contextSize));
  }
  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields(field));
  }
}


