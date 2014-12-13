package com.femineer.storm.logs;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ApacheLogLineParserBolt extends BaseBasicBolt {
  private static final Logger logger = Logger.getLogger("ApacheLogLineParserBolt");
  public static final String FIELD_IP_ADDRESS = "ipAddress";
  public static final String FIELD_CLIENT_IDENTD = "clientIdentd";
  public static final String FIELD_USER_ID= "userID";
  public static final String FIELD_DATE_TIME_STRING = "dateTimeString";
  public static final String FIELD_METHOD = "method";
  public static final String FIELD_ENDPOINT = "endpoint";
  public static final String FIELD_PROTOCOL = "protocol";
  public static final String FIELD_RESPONSE_CODE = "responseCode";
  public static final String FIELD_CONTENT_SIZE = "contentSize";

  // Example Apache log line:
  //   127.0.0.1 - - [21/Jul/2014:9:55:27 -0800] "GET /home.html HTTP/1.1" 200 2048
  private static final String LOG_ENTRY_PATTERN =
      // 1:IP  2:client 3:user 4:date time                   5:method 6:req 7:proto   8:respcode 9:size
      "^(\\S+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(\\S+) (\\S+) (\\S+)\" (\\d{3}) (\\S+)";
  private static final Pattern PATTERN = Pattern.compile(LOG_ENTRY_PATTERN);

  public static class ApacheAccessLog {
    private String ipAddress;
    private String clientIdentd;
    private String userID;
    private String dateTimeString;
    private String method;
    private String endpoint;
    private String protocol;
    private int responseCode;
    private long contentSize;

    private ApacheAccessLog(String ipAddress, String clientIdentd, String userID,
                            String dateTime, String method, String endpoint,
                            String protocol, String responseCode,
                            String contentSize) {
      this.ipAddress = ipAddress;
      this.clientIdentd = clientIdentd;
      this.userID = userID;
      this.dateTimeString = dateTime;  // TODO: Parse from dateTime String;
      this.method = method;
      this.endpoint = endpoint;
      this.protocol = protocol;
      this.responseCode = Integer.parseInt(responseCode);
      if (contentSize.equals("-")) {
        this.contentSize = 0;
      } else {
        this.contentSize = Long.parseLong(contentSize);
      }
    }

    public String getIpAddress() {
      return ipAddress;
    }

    public String getClientIdentd() {
      return clientIdentd;
    }

    public String getUserID() {
      return userID;
    }

    public String getDateTimeString() {
      return dateTimeString;
    }

    public String getMethod() {
      return method;
    }

    public String getEndpoint() {
      return endpoint;
    }

    public String getProtocol() {
      return protocol;
    }

    public int getResponseCode() {
      return responseCode;
    }

    public long getContentSize() {
      return contentSize;
    }
  }

  public static ApacheAccessLog parseFromLogLine(String logline) {
    Matcher m = PATTERN.matcher(logline);
    if (!m.find()) {
      logger.log(Level.ALL, "Cannot parse logline" + logline);
      throw new RuntimeException("Error parsing logline");
    }

    return new ApacheAccessLog(m.group(1), m.group(2), m.group(3), m.group(4),
        m.group(5), m.group(6), m.group(7), m.group(8), m.group(9));
  }

  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {
    String logline = tuple.getString(0);

    ApacheAccessLog accessLog = parseFromLogLine(logline);
    collector.emit(new Values(accessLog.getIpAddress(), accessLog.getClientIdentd(), accessLog.getUserID(),
        accessLog.getDateTimeString(), accessLog.getMethod(), accessLog.getEndpoint(), accessLog.getProtocol(),
        accessLog.getResponseCode(), accessLog.getContentSize()));
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields(FIELD_IP_ADDRESS, FIELD_CLIENT_IDENTD, FIELD_USER_ID,
        FIELD_DATE_TIME_STRING, FIELD_METHOD, FIELD_ENDPOINT, FIELD_PROTOCOL,
        FIELD_RESPONSE_CODE, FIELD_CONTENT_SIZE));
  }
}
