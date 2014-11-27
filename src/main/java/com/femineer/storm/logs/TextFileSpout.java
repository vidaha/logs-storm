package com.femineer.storm.logs;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

public class TextFileSpout extends BaseRichSpout {
  private SpoutOutputCollector _collector;
  private String filename;
  private BufferedReader bufferedReader;

  public TextFileSpout(String filename) {
    this.filename = filename;
  }

  @Override
  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
    _collector = collector;
    try {
      bufferedReader = new BufferedReader(new FileReader(filename));
    } catch (FileNotFoundException e) {
      // TODO: Handle this better.
      throw new RuntimeException(e);
    }
  }

  @Override
  public void nextTuple() {
    Utils.sleep(100);
    try {
      String line = bufferedReader.readLine();
      if (line != null) {
        _collector.emit(new Values(line));
      } else {
        // TODO: Handle this better.
        bufferedReader.close();
      }
    } catch (IOException e) {
      // TODO: Handle this better.
    }
  }

  @Override
  public void ack(Object id) {
  }

  @Override
  public void fail(Object id) {
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("line"));
  }

}
