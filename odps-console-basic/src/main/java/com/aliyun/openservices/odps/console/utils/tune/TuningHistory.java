package com.aliyun.openservices.odps.console.utils.tune;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

public class TuningHistory {
  public List<TuningRecord> records;
  public String tuningSql;
  public int maxSqlPreviewLines = 50;
  public int maxSqlPreviewLength = 1000;

  public static TuningHistory of(String explainTuneDesc, String tuningSql) {
    TuningHistory tuningHistory = new TuningHistory();
    tuningHistory.records = new ArrayList<>();
    tuningHistory.tuningSql = tuningSql;

    if(explainTuneDesc == null){
      return tuningHistory;
    }

    // Split explainTuneDesc into individual Candidate Plan descriptions
    String[] planSections = explainTuneDesc.split("Candidate Plan #");

    // Parse each Candidate Plan description and add TuningRecord to history
    for (int i = 1; i < planSections.length; i++) { // skip the first split part
      String candidatePlanDesc = planSections[i];
      TuningRecord record = TuningRecord.of(candidatePlanDesc);
      if (record != null) {
        tuningHistory.records.add(record);
      }
    }
    return tuningHistory;
  }

  public String toTuningReport(boolean reportForHuman, boolean onlyShowBetterPlan) {
    // sort it by latency
    List<TuningRecord> reportRecords =
        records.stream()
            .sorted(
                (l, r) -> {
                  long lt = l.getLatency();
                  long rt = r.getLatency();
                  if (lt < 0) {
                    lt = Long.MAX_VALUE;
                  }
                  if (rt < 0) {
                    rt = Long.MAX_VALUE;
                  }
                  // valid smaller time first
                  int ret = Long.compare(lt, rt);
                  if (ret != 0) {
                    return ret;
                  }
                  // current setting first
                  return -Boolean.compare(l.isCurrentSetting, r.isCurrentSetting);
                })
            .collect(Collectors.toList());

    long bestLatency = reportRecords.get(0).getLatency(); // return -1 if no valid latency
    int reportSize = reportRecords.size();

    double currentCpuCost = -1;
    long currentLatency = -1;
    for (TuningRecord record : reportRecords) {
      if (record.resourceCost == null) {
        record.resourceCost = ResourceCost.makeUnknownCost("Unknown");
      }
      if (record.isCurrentSetting) {
        currentCpuCost = record.resourceCost.getCpuValue();
        currentLatency = record.getLatency();
      }
    }

    if (reportForHuman && onlyShowBetterPlan) {
      // remove plan with worse cpu and latency
      double finalCurrentCpuCost = currentCpuCost;
      long finalCurrentLatency = currentLatency;
      reportRecords = reportRecords.stream()
          .filter(
              record -> {
                // invalid records, just print them all
                if (finalCurrentCpuCost < 0
                    || finalCurrentLatency < 0
                    || record.getLatency() < 0
                    || record.resourceCost.getCpuValue() < 0) {
                  return true;
                }
                // remove plan with worse cpu and latency
                if (record.getLatency() > finalCurrentLatency
                        && record.resourceCost.getCpuValue() > finalCurrentCpuCost) {
                  return false;
                }
                return true;
              })
          .collect(Collectors.toList());
    }

    int planNotDisplayed = reportSize - reportRecords.size();

    return makeReport(reportRecords, bestLatency, reportForHuman, planNotDisplayed);
  }


  private String makeReport(List<TuningRecord> sortedRecords, long bestLatency,
                            boolean reportForHuman, int planNotDisplayed) {
    StringBuilder report = new StringBuilder();
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    report.append("TUNING REPORT:\n");
    report.append("-----------------------------------------\n");
    // 检查SQL是否太长或者行数太多, 不利于展示
    String[] sqlLines = tuningSql.split("\\r?\\n");
    if(reportForHuman && (tuningSql.length() > maxSqlPreviewLength || sqlLines.length > maxSqlPreviewLines)) {
      report.append("Tuning SQL:\n");
      StringBuilder preview = new StringBuilder();
      if (tuningSql.length() > maxSqlPreviewLength) {
        preview.append(tuningSql.substring(0, maxSqlPreviewLength) +
                "... [" + (tuningSql.length() - maxSqlPreviewLength)+" characters not displayed]");
      } else {
        for (int i = 0; i < maxSqlPreviewLines; i++) {
          preview.append(sqlLines[i] + "\n");
        }
        preview.append(" [" + (sqlLines.length - maxSqlPreviewLines) + " lines not displayed]");
      }
      report.append(preview).append("\n");
    } else {
      report.append("Tuning SQL:\n").append(tuningSql).append("\n");
    }
    report.append("-----------------------------------------\n");
    for (TuningRecord record : sortedRecords) {
      if(record.isExplainTune) {
        continue;
      }
      String startTime = record.beginTimestamp > 0 ? sdf.format(new Date(record.beginTimestamp)) : "N/A";
      String endTime = record.endTimestamp > 0 ? sdf.format(new Date(record.endTimestamp)) : "N/A";
      long latency = record.endTimestamp - record.beginTimestamp;
      String latencyStr = latency > 0 ? latency + " ms" : "N/A";
      String latencyIncreasePctStr = "N/A";
      if(bestLatency > 0 && latency > 0) {
        double latencyIncreasePct =  (latency - bestLatency) * 100.0 / bestLatency;
        latencyIncreasePctStr = String.format("%.2f%%", latencyIncreasePct);
      }
      report.append("Candidate Plan #").append(record.planNumber).append(":\n");
      report.append("Tuning Hints: ").append(record.hintToSetting());
      report.append("Is Current Setting: ").append(record.isCurrentSetting).append("\n");
      report.append("Start Time: ").append(startTime).append("\n");
      report.append("End Time: ").append(endTime).append("\n");
      report.append("Latency: ").append(latencyStr).append("\n");
      report.append("Latency Increase: ").append(latencyIncreasePctStr).append("\n");
      report.append("Resource Cost: ").append(record.resourceCost.getFormattedString()).append("\n");
      // 当reportForHuman为true时，不打印Plan详情
      if (!reportForHuman) {
        report.append("Plan: \n").append(record.candidatePlan).append("\n");
      }
      report.append("Logview: ").append(record.logviewUrl).append("\n");
      report.append("-----------------------------------------\n");
    }
    if (planNotDisplayed > 0) {
      report.append(planNotDisplayed).append(" plan(s) with suboptimal latency/resource relative to " +
              "current setting plan were not displayed.\n");
    }
    return report.toString();
  }
}