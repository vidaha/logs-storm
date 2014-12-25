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
public class OutputFieldOfTuple extends BaseBasicBolt {
  private String field;
  private Class type;

  public OutputFieldOfTuple(String field, Class type) {
    this.field = field;
    this.type = type;
  }

  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {
    Values emitOutput = null;
    if (type == Long.class) {
      emitOutput = new Values(tuple.getLongByField(field));
    } else if (type == String.class) {
      emitOutput = new Values(tuple.getStringByField(field));
    } else if (type == Integer.class) {
      emitOutput = new Values(tuple.getIntegerByField(field));
    } else {
      throw new RuntimeException("Unsupported Type: " + type);
    }
    collector.emit(emitOutput);
  }
  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields(field));
  }
}


