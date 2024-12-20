package com.aliyun.openservices.odps.console.utils.tune;

import com.aliyun.odps.Instance;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TuningRecord {
  // planning info
  public int planNumber;
  public String candidatePlan;
  public Map<String, String> hints;
  public String estimatedCost;
  public boolean isCurrentSetting;
  public boolean isExplainTune;
  // running info
  public String taskName;
  public String instanceId;
  public String result;
  public String logviewUrl;
  public String taskDetailJson; // may need this in future
  public long beginTimestamp;
  // public long instanceEndTimestamp;
  public long endTimestamp;
  public ResourceCost resourceCost;

  public TuningRecord(
      int planNumber,
      String candidatePlan,
      Map<String, String> hints,
      String estimatedCost,
      boolean isCurrentSetting,
      boolean isExplainTune) {
    this.planNumber = planNumber;
    this.candidatePlan = candidatePlan;
    this.hints = hints;
    this.estimatedCost = estimatedCost;
    this.isCurrentSetting = isCurrentSetting;
    this.isExplainTune = isExplainTune;
  }

  public long getLatency() {
    long latency = endTimestamp - beginTimestamp;
    if (latency <= 0) {
        return -1;
    }
    return latency;
  }

  String hintToSetting() {
    StringBuilder setting = new StringBuilder();
    hints.forEach( (k,v) -> {
      setting.append("set " + k + "=" + v + ";\n");
    });
    return " {\n" + setting + "}\n";
  }

  public static TuningRecord of(String candidatePlanDesc) {
    Pattern pattern =
        Pattern.compile(
            "(\\d+)\\s+"
                + // Plan number
                "EstimatedCost: (.*?), IsCurrentSetting: (true|false)\\s+"
                + // matches the cost as a String
                "TuningHints: \\{(.*?)\\}\\s*Plan: (.+?)(?=-----|$)", // Tuning hints and candidate
                                                                      // plan
            Pattern.DOTALL | Pattern.CASE_INSENSITIVE);
    Matcher matcher = pattern.matcher(candidatePlanDesc);

    if (matcher.find()) {
      int planNumber = Integer.parseInt(matcher.group(1));
      String estimatedCost = matcher.group(2);
      boolean isCurrentSetting = Boolean.parseBoolean(matcher.group(3));
      String hintsString = matcher.group(4);
      String plan = matcher.group(5).trim();

      // Parse tuning hints
      Map<String, String> hints = new HashMap<>();
      Pattern hintPattern = Pattern.compile("set (\\w+(?:\\.\\w+)*?)=(.*?);");
      Matcher hintMatcher = hintPattern.matcher(hintsString);
      while (hintMatcher.find()) {
        hints.put(hintMatcher.group(1), hintMatcher.group(2));
      }

      // Create and return TuningRecord instance
      return new TuningRecord(planNumber, plan, hints, estimatedCost, isCurrentSetting, false);
    }
    return null;
  }
}