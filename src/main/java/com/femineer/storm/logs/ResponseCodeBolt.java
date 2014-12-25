package com.femineer.storm.logs;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

public class ResponseCodeBolt extends BaseBasicBolt {
  public static final String FIELD_RESPONSE_CODE = "response_code";
  public static final String FIELD_RESPONSE_CODE_COUNT = "response_code_count";

  public Map<Integer, Long> runningReponseCode = new HashMap<Integer, Long>();

  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {
    Integer responseCode = tuple.getInteger(0);

    if (runningReponseCode.containsKey(responseCode)) {
      runningReponseCode.put(responseCode, runningReponseCode.get(responseCode) + 1);
    } else {
      runningReponseCode.put(responseCode, 1L);
    }

    for (Map.Entry<Integer, Long> longLongEntry : runningReponseCode.entrySet()) {
      collector.emit(new Values(longLongEntry.getKey(), longLongEntry.getValue()));
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields(FIELD_RESPONSE_CODE, FIELD_RESPONSE_CODE_COUNT));
  }
}
