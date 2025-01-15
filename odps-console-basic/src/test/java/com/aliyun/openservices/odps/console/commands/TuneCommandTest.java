/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.aliyun.openservices.odps.console.commands;

import static org.junit.Assert.assertNotNull;

import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.utils.tune.ResourceCost;
import com.aliyun.openservices.odps.console.utils.tune.TuningHistory;
import com.aliyun.openservices.odps.console.utils.tune.TuningRecord;
import org.junit.Assert;
import org.junit.Test;

public class TuneCommandTest {
  private String[] positive = {"tune select 1;"};

  private String[] negative = {"tune    "};

  @Test
  public void test() throws Exception {
    TuneCommand tuneCommand = null;
    ExecutionContext context = ExecutionContext.init();

    for (String cmd : positive) {
      tuneCommand = TuneCommand.parse(cmd, context);
      assertNotNull(tuneCommand);
    }

    int errorCount = 0;

    for (String cmd : negative) {

      try {
        tuneCommand = TuneCommand.parse(cmd, context);
      } catch (ODPSConsoleException e) {
        ++errorCount;
      }
    }

    Assert.assertEquals(errorCount, negative.length);
  }

  @Test
  public void testHistory() {
    String sql =
        "select t.a_string, t.a_bigint + t2.a_double as x \n"
            + "from test t inner join test t2 on t.a_datetime = t2.a_datetime;";
    String explain =
        "Session Current Tuning Setting:  {\n"
            + "set odps.optimizer.cbo.rule.filter.black=re,ppr,vr;\n"
            + "}\n"
            + "TuningHints will overwrite session settings during candidate plan enumeration.\n"
            + "-----------------------------------------\n"
            + "Candidate Plan #1\n"
            + "EstimatedCost: 3.110400135319662E20, IsCurrentSetting: false\n"
            + "TuningHints: {\n"
            + "set odps.advanced.agg.join.transpose=true;\n"
            + "set odps.optimizer.auto.mapjoin.threshold=26214400;\n"
            + "set odps.optimizer.cbo.remove.shuffle=true;\n"
            + "set odps.optimizer.cbo.rule.filter.black=MJR;\n"
            + "set odps.optimizer.cbo.rule.filter.white=PHJ;\n"
            + "set odps.optimizer.enable.correlated.shuffle=PASSIVE;\n"
            + "set odps.optimizer.force.dynamic.filter=false;\n"
            + "set odps.optimizer.spool.enabled.adaptive=true;\n"
            + "}\n"
            + "Plan: \n"
            + "OdpsPhysicalAdhocSink(fields=[[0, 1]])\n"
            + "  OdpsPhysicalProject(a_string=[$1], x=[PLUS(TODOUBLE($0), $3)])\n"
            + "    OdpsPhysicalHashJoin(type=[INNER], equi=[[($2,$4)]], mainstream=[0])\n"
            + "      OdpsPhysicalStreamlineRead(order=[[]])\n"
            + "        OdpsPhysicalStreamlineWrite(shuffle=[hash[2]], order=[[]])\n"
            + "          OdpsPhysicalFilter(condition=[ISNOTNULL($2)])\n"
            + "            OdpsPhysicalTableScan(table=[[test_project.test, a_bigint,a_string,a_datetime(3) {0, 1, 3}]])\n"
            + "      OdpsPhysicalStreamlineRead(order=[[]])\n"
            + "        OdpsPhysicalStreamlineWrite(shuffle=[hash[1]], order=[[]])\n"
            + "          OdpsPhysicalFilter(condition=[ISNOTNULL($1)])\n"
            + "            OdpsPhysicalTableScan(table=[[test_project.test, a_double,a_datetime(2) {2, 3}]])\n"
            + "-----------------------------------------\n"
            + "Candidate Plan #2\n"
            + "EstimatedCost: 3.110400135319662E20, IsCurrentSetting: false\n"
            + "TuningHints: {\n"
            + "set odps.advanced.agg.join.transpose=true;\n"
            + "set odps.optimizer.auto.mapjoin.threshold=26214400;\n"
            + "set odps.optimizer.cbo.remove.shuffle=false;\n"
            + "set odps.optimizer.cbo.rule.filter.black=MJR;\n"
            + "set odps.optimizer.cbo.rule.filter.white=PHJ;\n"
            + "set odps.optimizer.enable.correlated.shuffle=PASSIVE;\n"
            + "set odps.optimizer.force.dynamic.filter=false;\n"
            + "set odps.optimizer.spool.enabled.adaptive=true;\n"
            + "}\n"
            + "Plan: \n"
            + "OdpsPhysicalAdhocSink(fields=[[0, 1]])\n"
            + "  OdpsPhysicalProject(a_string=[$1], x=[PLUS(TODOUBLE($0), $3)])\n"
            + "    OdpsPhysicalHashJoin(type=[INNER], equi=[[($2,$4)]], mainstream=[0])\n"
            + "      OdpsPhysicalStreamlineRead(order=[[]])\n"
            + "        OdpsPhysicalStreamlineWrite(shuffle=[hash[2],JoinHasher], order=[[]])\n"
            + "          OdpsPhysicalFilter(condition=[ISNOTNULL($2)])\n"
            + "            OdpsPhysicalTableScan(table=[[test_project.test, a_bigint,a_string,a_datetime(3) {0, 1, 3}]])\n"
            + "      OdpsPhysicalStreamlineRead(order=[[]])\n"
            + "        OdpsPhysicalStreamlineWrite(shuffle=[hash[1],JoinHasher], order=[[]])\n"
            + "          OdpsPhysicalFilter(condition=[ISNOTNULL($1)])\n"
            + "            OdpsPhysicalTableScan(table=[[test_project.test, a_double,a_datetime(2) {2, 3}]])\n"
            + "-----------------------------------------\n"
            + "Candidate Plan #3\n"
            + "EstimatedCost: 3.110400170245813E20, IsCurrentSetting: false\n"
            + "TuningHints: {\n"
            + "set odps.advanced.agg.join.transpose=true;\n"
            + "set odps.optimizer.auto.mapjoin.threshold=26214400;\n"
            + "set odps.optimizer.cbo.remove.shuffle=false;\n"
            + "set odps.optimizer.cbo.rule.filter.black=;\n"
            + "set odps.optimizer.cbo.rule.filter.white=;\n"
            + "set odps.optimizer.enable.correlated.shuffle=PASSIVE;\n"
            + "set odps.optimizer.force.dynamic.filter=false;\n"
            + "set odps.optimizer.spool.enabled.adaptive=true;\n"
            + "}\n"
            + "Plan: \n"
            + "OdpsPhysicalAdhocSink(fields=[[0, 1]])\n"
            + "  OdpsPhysicalProject(a_string=[$1], x=[PLUS(TODOUBLE($0), $3)])\n"
            + "    OdpsPhysicalMergeJoin(type=[INNER], equi=[[($2,$4)]])\n"
            + "      OdpsPhysicalStreamlineRead(order=[[2]])\n"
            + "        OdpsPhysicalStreamlineWrite(shuffle=[hash[2],JoinHasher], order=[[2]])\n"
            + "          OdpsPhysicalFilter(condition=[ISNOTNULL($2)])\n"
            + "            OdpsPhysicalTableScan(table=[[test_project.test, a_bigint,a_string,a_datetime(3) {0, 1, 3}]])\n"
            + "      OdpsPhysicalStreamlineRead(order=[[1]])\n"
            + "        OdpsPhysicalStreamlineWrite(shuffle=[hash[1],JoinHasher], order=[[1]])\n"
            + "          OdpsPhysicalFilter(condition=[ISNOTNULL($1)])\n"
            + "            OdpsPhysicalTableScan(table=[[test_project.test, a_double,a_datetime(2) {2, 3}]])\n"
            + "-----------------------------------------\n"
            + "Candidate Plan #4\n"
            + "EstimatedCost: 3.110400170245813E20, IsCurrentSetting: true\n"
            + "TuningHints: {\n"
            + "set odps.advanced.agg.join.transpose=true;\n"
            + "set odps.optimizer.auto.mapjoin.threshold=26214400;\n"
            + "set odps.optimizer.cbo.remove.shuffle=true;\n"
            + "set odps.optimizer.cbo.rule.filter.black=;\n"
            + "set odps.optimizer.cbo.rule.filter.white=;\n"
            + "set odps.optimizer.enable.correlated.shuffle=PASSIVE;\n"
            + "set odps.optimizer.force.dynamic.filter=false;\n"
            + "set odps.optimizer.spool.enabled.adaptive=true;\n"
            + "}\n"
            + "Plan: \n"
            + "OdpsPhysicalAdhocSink(fields=[[0, 1]])\n"
            + "  OdpsPhysicalProject(a_string=[$1], x=[PLUS(TODOUBLE($0), $3)])\n"
            + "    OdpsPhysicalMergeJoin(type=[INNER], equi=[[($2,$4)]])\n"
            + "      OdpsPhysicalStreamlineRead(order=[[2]])\n"
            + "        OdpsPhysicalStreamlineWrite(shuffle=[hash[2]], order=[[2]])\n"
            + "          OdpsPhysicalFilter(condition=[ISNOTNULL($2)])\n"
            + "            OdpsPhysicalTableScan(table=[[test_project.test, a_bigint,a_string,a_datetime(3) {0, 1, 3}]])\n"
            + "      OdpsPhysicalStreamlineRead(order=[[1]])\n"
            + "        OdpsPhysicalStreamlineWrite(shuffle=[hash[1]], order=[[1]])\n"
            + "          OdpsPhysicalFilter(condition=[ISNOTNULL($1)])\n"
            + "            OdpsPhysicalTableScan(table=[[test_project.test, a_double,a_datetime(2) {2, 3}]])\n"
            + "-----------------------------------------";
    TuningHistory history = TuningHistory.of(explain, sql);
    Assert.assertEquals(history.records.size(), 4);
    TuningRecord record1 = history.records.get(0);

    ResourceCost cost = ResourceCost.fromText("resource cost: cpu 1.00 Core * Min, memory 1.00 GB * Min");
    ResourceCost cost2 = ResourceCost.fromText("resource cost: cpu 1.01 Core * Min, memory 1.00 GB * Min");

    record1.taskName = "taskName1";
    record1.beginTimestamp = 100000;
    record1.instanceId = "instanceId1";
    record1.result = "result1";
    record1.endTimestamp = 200000;
    record1.logviewUrl = "logview1";
    record1.resourceCost = cost2;
    assert !record1.isCurrentSetting;

    TuningRecord record2 = history.records.get(1);
    record2.taskName = "taskName2";
    record2.beginTimestamp = 300000;
    // failed
    assert !record2.isCurrentSetting;

    TuningRecord record3 = history.records.get(2);
    record3.taskName = "taskName3";
    record3.beginTimestamp = 400000;
    record3.instanceId = "instanceId3";
    record3.endTimestamp = 450000;
    record3.resourceCost = cost;
    assert !record3.isCurrentSetting;

    // best time and is current setting
    TuningRecord record4 = history.records.get(3);
    record4.taskName = "taskName4";
    record4.beginTimestamp = 500000;
    record4.instanceId = "instanceId4";
    record4.endTimestamp = 550000;
    record4.resourceCost = cost;
    assert record4.isCurrentSetting;

    String tuningReport =
        "TUNING REPORT:\n"
            + "-----------------------------------------\n"
            + "Tuning SQL:\n"
            + "select t.a_string, t.a_bigint + t2.a_double as x \n"
            + "from test t inner join test t2 on t.a_datetime = t2.a_datetime;\n"
            + "-----------------------------------------\n"
            + "Candidate Plan #4:\n"
            + "Tuning Hints:  {\n"
            + "set odps.optimizer.cbo.rule.filter.white=;\n"
            + "set odps.advanced.agg.join.transpose=true;\n"
            + "set odps.optimizer.auto.mapjoin.threshold=26214400;\n"
            + "set odps.optimizer.force.dynamic.filter=false;\n"
            + "set odps.optimizer.spool.enabled.adaptive=true;\n"
            + "set odps.optimizer.cbo.rule.filter.black=;\n"
            + "set odps.optimizer.enable.correlated.shuffle=PASSIVE;\n"
            + "set odps.optimizer.cbo.remove.shuffle=true;\n"
            + "}\n"
            + "Is Current Setting: true\n"
            + "Start Time: 1970-01-01 08:08:20\n"
            + "End Time: 1970-01-01 08:09:10\n"
            + "Latency: 50000 ms\n"
            + "Latency Increase: 0.00%\n"
            + "Resource Cost: cpu 1.00 Core * Min, memory 1.00 GB * Min\n"
            + "Plan: \n"
            + "OdpsPhysicalAdhocSink(fields=[[0, 1]])\n"
            + "  OdpsPhysicalProject(a_string=[$1], x=[PLUS(TODOUBLE($0), $3)])\n"
            + "    OdpsPhysicalMergeJoin(type=[INNER], equi=[[($2,$4)]])\n"
            + "      OdpsPhysicalStreamlineRead(order=[[2]])\n"
            + "        OdpsPhysicalStreamlineWrite(shuffle=[hash[2]], order=[[2]])\n"
            + "          OdpsPhysicalFilter(condition=[ISNOTNULL($2)])\n"
            + "            OdpsPhysicalTableScan(table=[[test_project.test, a_bigint,a_string,a_datetime(3) {0, 1, 3}]])\n"
            + "      OdpsPhysicalStreamlineRead(order=[[1]])\n"
            + "        OdpsPhysicalStreamlineWrite(shuffle=[hash[1]], order=[[1]])\n"
            + "          OdpsPhysicalFilter(condition=[ISNOTNULL($1)])\n"
            + "            OdpsPhysicalTableScan(table=[[test_project.test, a_double,a_datetime(2) {2, 3}]])\n"
            + "Logview: null\n"
            + "-----------------------------------------\n"
            + "Candidate Plan #3:\n"
            + "Tuning Hints:  {\n"
            + "set odps.optimizer.cbo.rule.filter.white=;\n"
            + "set odps.advanced.agg.join.transpose=true;\n"
            + "set odps.optimizer.auto.mapjoin.threshold=26214400;\n"
            + "set odps.optimizer.force.dynamic.filter=false;\n"
            + "set odps.optimizer.spool.enabled.adaptive=true;\n"
            + "set odps.optimizer.cbo.rule.filter.black=;\n"
            + "set odps.optimizer.enable.correlated.shuffle=PASSIVE;\n"
            + "set odps.optimizer.cbo.remove.shuffle=false;\n"
            + "}\n"
            + "Is Current Setting: false\n"
            + "Start Time: 1970-01-01 08:06:40\n"
            + "End Time: 1970-01-01 08:07:30\n"
            + "Latency: 50000 ms\n"
            + "Latency Increase: 0.00%\n"
            + "Resource Cost: cpu 1.00 Core * Min, memory 1.00 GB * Min\n"
            + "Plan: \n"
            + "OdpsPhysicalAdhocSink(fields=[[0, 1]])\n"
            + "  OdpsPhysicalProject(a_string=[$1], x=[PLUS(TODOUBLE($0), $3)])\n"
            + "    OdpsPhysicalMergeJoin(type=[INNER], equi=[[($2,$4)]])\n"
            + "      OdpsPhysicalStreamlineRead(order=[[2]])\n"
            + "        OdpsPhysicalStreamlineWrite(shuffle=[hash[2],JoinHasher], order=[[2]])\n"
            + "          OdpsPhysicalFilter(condition=[ISNOTNULL($2)])\n"
            + "            OdpsPhysicalTableScan(table=[[test_project.test, a_bigint,a_string,a_datetime(3) {0, 1, 3}]])\n"
            + "      OdpsPhysicalStreamlineRead(order=[[1]])\n"
            + "        OdpsPhysicalStreamlineWrite(shuffle=[hash[1],JoinHasher], order=[[1]])\n"
            + "          OdpsPhysicalFilter(condition=[ISNOTNULL($1)])\n"
            + "            OdpsPhysicalTableScan(table=[[test_project.test, a_double,a_datetime(2) {2, 3}]])\n"
            + "Logview: null\n"
            + "-----------------------------------------\n"
            + "Candidate Plan #1:\n"
            + "Tuning Hints:  {\n"
            + "set odps.optimizer.cbo.rule.filter.white=PHJ;\n"
            + "set odps.advanced.agg.join.transpose=true;\n"
            + "set odps.optimizer.auto.mapjoin.threshold=26214400;\n"
            + "set odps.optimizer.force.dynamic.filter=false;\n"
            + "set odps.optimizer.spool.enabled.adaptive=true;\n"
            + "set odps.optimizer.cbo.rule.filter.black=MJR;\n"
            + "set odps.optimizer.enable.correlated.shuffle=PASSIVE;\n"
            + "set odps.optimizer.cbo.remove.shuffle=true;\n"
            + "}\n"
            + "Is Current Setting: false\n"
            + "Start Time: 1970-01-01 08:01:40\n"
            + "End Time: 1970-01-01 08:03:20\n"
            + "Latency: 100000 ms\n"
            + "Latency Increase: 100.00%\n"
            + "Resource Cost: cpu 1.01 Core * Min, memory 1.00 GB * Min\n"
            + "Plan: \n"
            + "OdpsPhysicalAdhocSink(fields=[[0, 1]])\n"
            + "  OdpsPhysicalProject(a_string=[$1], x=[PLUS(TODOUBLE($0), $3)])\n"
            + "    OdpsPhysicalHashJoin(type=[INNER], equi=[[($2,$4)]], mainstream=[0])\n"
            + "      OdpsPhysicalStreamlineRead(order=[[]])\n"
            + "        OdpsPhysicalStreamlineWrite(shuffle=[hash[2]], order=[[]])\n"
            + "          OdpsPhysicalFilter(condition=[ISNOTNULL($2)])\n"
            + "            OdpsPhysicalTableScan(table=[[test_project.test, a_bigint,a_string,a_datetime(3) {0, 1, 3}]])\n"
            + "      OdpsPhysicalStreamlineRead(order=[[]])\n"
            + "        OdpsPhysicalStreamlineWrite(shuffle=[hash[1]], order=[[]])\n"
            + "          OdpsPhysicalFilter(condition=[ISNOTNULL($1)])\n"
            + "            OdpsPhysicalTableScan(table=[[test_project.test, a_double,a_datetime(2) {2, 3}]])\n"
            + "Logview: logview1\n"
            + "-----------------------------------------\n"
            + "Candidate Plan #2:\n"
            + "Tuning Hints:  {\n"
            + "set odps.optimizer.cbo.rule.filter.white=PHJ;\n"
            + "set odps.advanced.agg.join.transpose=true;\n"
            + "set odps.optimizer.auto.mapjoin.threshold=26214400;\n"
            + "set odps.optimizer.force.dynamic.filter=false;\n"
            + "set odps.optimizer.spool.enabled.adaptive=true;\n"
            + "set odps.optimizer.cbo.rule.filter.black=MJR;\n"
            + "set odps.optimizer.enable.correlated.shuffle=PASSIVE;\n"
            + "set odps.optimizer.cbo.remove.shuffle=false;\n"
            + "}\n"
            + "Is Current Setting: false\n"
            + "Start Time: 1970-01-01 08:05:00\n"
            + "End Time: N/A\n"
            + "Latency: N/A\n"
            + "Latency Increase: N/A\n"
            + "Resource Cost: Unknown\n"
            + "Plan: \n"
            + "OdpsPhysicalAdhocSink(fields=[[0, 1]])\n"
            + "  OdpsPhysicalProject(a_string=[$1], x=[PLUS(TODOUBLE($0), $3)])\n"
            + "    OdpsPhysicalHashJoin(type=[INNER], equi=[[($2,$4)]], mainstream=[0])\n"
            + "      OdpsPhysicalStreamlineRead(order=[[]])\n"
            + "        OdpsPhysicalStreamlineWrite(shuffle=[hash[2],JoinHasher], order=[[]])\n"
            + "          OdpsPhysicalFilter(condition=[ISNOTNULL($2)])\n"
            + "            OdpsPhysicalTableScan(table=[[test_project.test, a_bigint,a_string,a_datetime(3) {0, 1, 3}]])\n"
            + "      OdpsPhysicalStreamlineRead(order=[[]])\n"
            + "        OdpsPhysicalStreamlineWrite(shuffle=[hash[1],JoinHasher], order=[[]])\n"
            + "          OdpsPhysicalFilter(condition=[ISNOTNULL($1)])\n"
            + "            OdpsPhysicalTableScan(table=[[test_project.test, a_double,a_datetime(2) {2, 3}]])\n"
            + "Logview: null\n"
            + "-----------------------------------------\n";
    Assert.assertEquals(tuningReport, history.toTuningReport(false, false));

    // test human print

    String humanHead = "TUNING REPORT:\n" +
            "-----------------------------------------\n" +
            "Tuning SQL:\n" +
            "select t.a_string, t.a_bigint + t2.a_double as x \n" +
            "from test t inner join test t2 on t.a_datetime = t2.a_datetime;\n" ;
    String humanTrails = "-----------------------------------------\n"
            + "Candidate Plan #4:\n"
            + "Tuning Hints:  {\n"
            + "set odps.optimizer.cbo.rule.filter.white=;\n"
            + "set odps.advanced.agg.join.transpose=true;\n"
            + "set odps.optimizer.auto.mapjoin.threshold=26214400;\n"
            + "set odps.optimizer.force.dynamic.filter=false;\n"
            + "set odps.optimizer.spool.enabled.adaptive=true;\n"
            + "set odps.optimizer.cbo.rule.filter.black=;\n"
            + "set odps.optimizer.enable.correlated.shuffle=PASSIVE;\n"
            + "set odps.optimizer.cbo.remove.shuffle=true;\n"
            + "}\n"
            + "Is Current Setting: true\n"
            + "Start Time: 1970-01-01 08:08:20\n"
            + "End Time: 1970-01-01 08:09:10\n"
            + "Latency: 50000 ms\n"
            + "Latency Increase: 0.00%\n"
            + "Resource Cost: cpu 1.00 Core * Min, memory 1.00 GB * Min\n"
            + "Logview: null\n"
            + "-----------------------------------------\n"
            + "Candidate Plan #3:\n"
            + "Tuning Hints:  {\n"
            + "set odps.optimizer.cbo.rule.filter.white=;\n"
            + "set odps.advanced.agg.join.transpose=true;\n"
            + "set odps.optimizer.auto.mapjoin.threshold=26214400;\n"
            + "set odps.optimizer.force.dynamic.filter=false;\n"
            + "set odps.optimizer.spool.enabled.adaptive=true;\n"
            + "set odps.optimizer.cbo.rule.filter.black=;\n"
            + "set odps.optimizer.enable.correlated.shuffle=PASSIVE;\n"
            + "set odps.optimizer.cbo.remove.shuffle=false;\n"
            + "}\n"
            + "Is Current Setting: false\n"
            + "Start Time: 1970-01-01 08:06:40\n"
            + "End Time: 1970-01-01 08:07:30\n"
            + "Latency: 50000 ms\n"
            + "Latency Increase: 0.00%\n"
            + "Resource Cost: cpu 1.00 Core * Min, memory 1.00 GB * Min\n"
            + "Logview: null\n"
            + "-----------------------------------------\n"
            + "Candidate Plan #2:\n"
            + "Tuning Hints:  {\n"
            + "set odps.optimizer.cbo.rule.filter.white=PHJ;\n"
            + "set odps.advanced.agg.join.transpose=true;\n"
            + "set odps.optimizer.auto.mapjoin.threshold=26214400;\n"
            + "set odps.optimizer.force.dynamic.filter=false;\n"
            + "set odps.optimizer.spool.enabled.adaptive=true;\n"
            + "set odps.optimizer.cbo.rule.filter.black=MJR;\n"
            + "set odps.optimizer.enable.correlated.shuffle=PASSIVE;\n"
            + "set odps.optimizer.cbo.remove.shuffle=false;\n"
            + "}\n"
            + "Is Current Setting: false\n"
            + "Start Time: 1970-01-01 08:05:00\n"
            + "End Time: N/A\n"
            + "Latency: N/A\n"
            + "Latency Increase: N/A\n"
            + "Resource Cost: Unknown\n"
            + "Logview: null\n"
            + "-----------------------------------------\n"
            + "1 plan(s) with suboptimal latency/resource relative to current setting plan were not displayed.\n";

    Assert.assertEquals(humanHead + humanTrails, history.toTuningReport(true, true));
    history.maxSqlPreviewLines = 1;
    String humanHead2 = "TUNING REPORT:\n" +
            "-----------------------------------------\n" +
            "Tuning SQL:\n" +
            "select t.a_string, t.a_bigint + t2.a_double as x \n" +
            " [1 lines not displayed]\n";

    Assert.assertEquals(humanHead2 + humanTrails, history.toTuningReport(true, true));
    history.maxSqlPreviewLines = 10;
    history.maxSqlPreviewLength = 100;
    String humanHead3 = "TUNING REPORT:\n" +
            "-----------------------------------------\n" +
            "Tuning SQL:\n" +
            "select t.a_string, t.a_bigint + t2.a_double as x \n" +
            "from test t inner join test t2 on t.a_datetime = t... [13 characters not displayed]\n";
    Assert.assertEquals(humanHead3 + humanTrails, history.toTuningReport(true, true));
  }
}