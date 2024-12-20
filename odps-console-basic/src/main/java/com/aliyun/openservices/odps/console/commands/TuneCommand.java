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

import com.aliyun.odps.*;
import com.aliyun.odps.task.SQLTask;
import com.aliyun.odps.task.SqlPlanTask;
import com.aliyun.odps.utils.StringUtils;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.constants.ODPSConsoleConstants;
import com.aliyun.openservices.odps.console.output.DefaultOutputWriter;
import com.aliyun.openservices.odps.console.output.InstanceRunner;
import com.aliyun.openservices.odps.console.output.state.InstanceSuccess;
import com.aliyun.openservices.odps.console.utils.ODPSConsoleUtils;
import com.aliyun.openservices.odps.console.utils.QueryUtil;
import com.aliyun.openservices.odps.console.utils.tune.ResourceCost;
import com.aliyun.openservices.odps.console.utils.tune.TuningHistory;
import com.aliyun.openservices.odps.console.utils.tune.TuningRecord;
import com.google.common.collect.ImmutableMap;
import org.jline.reader.UserInterruptException;

import java.io.PrintStream;
import java.util.*;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author wenzhuang.zwz
 * **/
public class TuneCommand extends MultiClusterCommandBase {

  private static final String PMC_TASK_CONSOLE_KEY = "odps.console.progressive.long.running.task";
  private static final String RESULT_CACHE_ENABLE_KEY = "odps.sql.session.result.cache.enable";
  private static final String TUNE_IGNORE_FAILURE_KEY = "odps.sql.tune.ignore.failure";
  private static final String TUNE_REPORT_FOR_HUMAN = "odps.sql.tune.report.for.human";
  private static final String ONLY_SHOW_BETTER_PLAN = "odps.sql.tune.only.show.better.plan";
  private static final String IS_CURRENT_SETTING = "odps.sql.tune.is.current.setting";
  public static final String[]
      HELP_TAGS =
      new String[]{"tune", "tuning"};


  public static void printUsage(PrintStream stream) {
    stream.println(
        "Usage: TUNE <sql>;");
  }

  private String taskName = "";
  private boolean isSelect = false;
  private boolean ignoreFailure = false;
  private boolean reportForHuman = true;
  private boolean onlyShowBetterPlan = true;

  public void run() throws OdpsException, ODPSConsoleException {

    ExecutionContext context = getContext();
    DefaultOutputWriter writer = context.getOutputWriter();

    // todo later: support sqa
    if (this.getContext().isInteractiveQuery()){
      throw new OdpsException("Tune command is not supported in interactive query mode.");
    }

    if ("true".equalsIgnoreCase(SetCommand.setMap.getOrDefault(PMC_TASK_CONSOLE_KEY, "false"))) {
      throw new OdpsException("Tune command is not supported in progressive mode.");
    }

    if ("true".equalsIgnoreCase(SetCommand.setMap.getOrDefault(RESULT_CACHE_ENABLE_KEY, "false"))) {
      throw new OdpsException("Tune command is not supported with result cache.");
    }

    if ("true".equalsIgnoreCase(SetCommand.setMap.getOrDefault(TUNE_IGNORE_FAILURE_KEY, "false"))) {
      this.ignoreFailure = true;
    }

    if ("false".equalsIgnoreCase(SetCommand.setMap.getOrDefault(TUNE_REPORT_FOR_HUMAN, "true"))) {
      this.reportForHuman = false;
    }
    if (!this.reportForHuman || "false".equalsIgnoreCase(SetCommand.setMap.getOrDefault(ONLY_SHOW_BETTER_PLAN, "true"))) {
      this.onlyShowBetterPlan = false;
    }

    if (context.isAsyncMode()) {
      throw new OdpsException("Tune command is not supported in async mode.");
    }

    if (isSelect) {
      writer.writeError("Tune command will not report select result to console.");
    }

    // get candidate plans
    String explainTuneSql = "EXPLAIN TUNE " + getCommandText();
    TuningRecord explainTuneRecord = new TuningRecord(0, "", null, "", true, true);
    runOneSqlTask(explainTuneSql, explainTuneRecord);

    TuningHistory history = TuningHistory.of(explainTuneRecord.result, getCommandText());
    if (history.records.isEmpty()) {
      throw new OdpsException("EXPLAIN TUNE cannot get valid query plan, explain query:" + explainTuneSql);
    }
    writer.writeError("RUN " + history.records.size() + " Candidate Plans Begin.");

    for (TuningRecord record : history.records) {
      runOneSqlTask(getCommandText(), record);
      if(record.endTimestamp == 0) {
        writer.writeError("RUN Candidate Plan #" + record.planNumber + " Failed.");
      } else {
        writer.writeError("RUN Candidate Plan #" + record.planNumber + " Done.");
      }
    }
    // todo later: dump to local dir/to table
    writer.writeResult(history.toTuningReport(this.reportForHuman, this.onlyShowBetterPlan));
    // no exception, print success
    writer.writeError("OK");
  }

  private void runOneSqlTask(String sql, TuningRecord tuningRecord) throws ODPSConsoleException {
    ExecutionContext context = getContext();
    DefaultOutputWriter writer = context.getOutputWriter();
    int retryTime = context.getRetryTimes();
    retryTime = retryTime > 0 ? retryTime : 1;
    boolean isDryRun = getContext().isDryRun(); // now it is always false
    // do retry
    while (retryTime > 0) {
      Task task = null;
      try {
        taskName = getTaskName(isDryRun);
        if (isDryRun) {
          task = new SqlPlanTask(taskName, sql);
        } else {
          task = new SQLTask();
          task.setName(taskName);
          ((SQLTask) task).setQuery(sql);
        }

        HashMap<String, String> taskConfig = QueryUtil.getTaskConfig();
        if (!getContext().isMachineReadable()) {
          Map<String, String> settings = new HashMap<>();
          settings.put("odps.sql.select.output.format", "HumanReadable");
          addSetting(taskConfig, settings);
        }

        addSetting(taskConfig, SetCommand.setMap);
        if(tuningRecord.hints != null) {
          addSetting(taskConfig, tuningRecord.hints);
          if (!tuningRecord.isExplainTune) {
            addSetting(taskConfig, ImmutableMap.of(IS_CURRENT_SETTING, String.valueOf(tuningRecord.isCurrentSetting)));
          }
        }

        for (Entry<String, String> property : taskConfig.entrySet()) {
          task.setProperty(property.getKey(), property.getValue());
        }

        runJob(task, tuningRecord);

        // success
        break;
      } catch (UserInterruptException e) {
        throw e;
      } catch (Exception e) {
        if (task instanceof SQLTask && QueryUtil.isOperatorDisabled(((SQLTask) task).getQuery())) {
          String errorMessage = e.getMessage();
          if (errorMessage.contains("ODPS-0110999")) {
            throw new ODPSConsoleException(e.getMessage());
          } else if (!errorMessage.contains("ODPS-")) {
            throw new ODPSConsoleException("ODPS-0110999:" + e.getMessage());
          }
        }

        retryTime--;
        if (retryTime == 0) {
          if(tuningRecord.isExplainTune || !ignoreFailure) {
            throw new ODPSConsoleException(e.getMessage(), e);
          }
          tuningRecord.endTimestamp = 0; // ignore failed
          return;
        }

        writer.writeError("retry " + retryTime);
        writer.writeDebug(StringUtils.stringifyException(e));
      }
    }
  }

  protected void runJob(Task task, TuningRecord record) throws OdpsException, ODPSConsoleException {
    record.taskName = taskName;
    ExecutionContext context = getContext();
    record.beginTimestamp = System.currentTimeMillis();
    InstanceRunner runner = new InstanceRunner(getCurrentOdps(), task, context);
    runner.submit();

    //delay hooks after print result
    OdpsHooks hooks = runner.getInstance().getOdpsHooks();
    runner.getInstance().setOdpsHooks(null);

    runner.waitForCompletion();
    instanceId = runner.getInstance().getId();

    StringBuilder builder = new StringBuilder();
    try {
      Iterator<String> queryResult = runner.getResult();

      while (queryResult.hasNext()) {
        ODPSConsoleUtils.checkThreadInterrupted();
        builder.append(queryResult.next()).append("\n");
      }
      Instance instance = runner.getInstance();
      record.instanceId = instanceId;
      record.result = builder.toString();
      record.endTimestamp = System.currentTimeMillis();
      // cannot get resource cost by a normal way ...
      //record.resourceCost = instance.getTaskCost(taskName);
      try {
        record.resourceCost = ResourceCost.fromText(InstanceSuccess.getTaskSummaryV1(
            instance.getOdps(), instance, taskName, context.getOutputWriter()).getSummaryText());
      } catch (Exception e) {
        record.resourceCost = ResourceCost.makeUnknownCost("Unknown cost, get task summary failed.");
      }
      record.logviewUrl = runner.getInstanceStateContext().getOdps()
              .logview().generateLogView(instance, context.getLogViewLife());

      // Instance.TaskSummary summary = instance.getTaskSummary(taskName);
      // todo later analyze json summary
      // record.taskDetailJson = instance.getTaskDetailJson2(record.taskName);
    } finally {
      if (hooks != null) {
        hooks.after(runner.getInstance(), getCurrentOdps());
      }
    }
  }

  protected String getTaskName(boolean isDryRun) {
    // use the same task name
    if (isDryRun) {
      return "console_sqlplan_task_" + Calendar.getInstance().getTimeInMillis();
    } else {
      return "console_query_task_" + Calendar.getInstance().getTimeInMillis();
    }
  }

  public String getTaskName() {
    return taskName;
  }

  public String getInstanceId() {
    return instanceId;
  }


  public TuneCommand(String command, ExecutionContext context) {
    super(command, context);

    if (command.toUpperCase().matches("^SELECT[\\s\\S]*")) {
      isSelect = true;
    }
  }

  public static TuneCommand parse(String commandString, ExecutionContext sessionContext)
      throws ODPSConsoleException {
    String regStr = "^\\s*(TUNE)(\\s+)([\\s\\S]*)";

    Pattern p = Pattern.compile(regStr, Pattern.CASE_INSENSITIVE);
    Matcher m = p.matcher(commandString);

    if (m.find()) {
      String sqlPart = m.group(3).trim();

      if (sqlPart.isEmpty()) {
        throw new ODPSConsoleException(ODPSConsoleConstants.BAD_COMMAND + "TUNE <sql>;");
      }

      sqlPart = sqlPart.trim();

      if (!sqlPart.endsWith(";")) {
        sqlPart = sqlPart + ";";
      }

      return new TuneCommand(sqlPart, sessionContext);
    }
    return null;
  }
}