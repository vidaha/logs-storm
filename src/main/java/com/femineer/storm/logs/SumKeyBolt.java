package com.femineer.storm.logs;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

public class SumKeyBolt extends BaseBasicBolt {
  public static final String FIELD_KEY = "key";
  public static final String FIELD_COUNT = "count";

  public Map<Object, Long> map = new HashMap<Object, Long>();

  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {
    Object key = tuple.getValue(0);

    if (map.containsKey(key)) {
      map.put(key, map.get(key) + 1);
    } else {
      map.put(key, 1L);
    }

    for (Map.Entry<Object, Long> entry : map.entrySet()) {
      collector.emit(new Values(entry.getKey(), entry.getValue()));
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields(FIELD_KEY, FIELD_COUNT));
  }
}
