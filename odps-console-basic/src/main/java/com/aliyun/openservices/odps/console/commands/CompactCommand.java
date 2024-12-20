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

import java.io.PrintStream;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jline.reader.UserInterruptException;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Table;
import com.aliyun.odps.Task;
import com.aliyun.odps.task.MergeTask;
import com.aliyun.odps.utils.StringUtils;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.constants.ODPSConsoleConstants;
import com.aliyun.openservices.odps.console.output.DefaultOutputWriter;
import com.aliyun.openservices.odps.console.utils.CommandParserUtils;
import com.aliyun.openservices.odps.console.utils.Coordinate;
import com.aliyun.openservices.odps.console.utils.QueryUtil;

/**
 * @author zhenhong.gzh
 **/
public class CompactCommand extends MultiClusterCommandBase {

  public static final String[] HELP_TAGS = new String[]{"merge", "table", "alter", "compact"};

  private String compactType;
  private Map<String, String> params;

  public static void printUsage(PrintStream stream) {
    stream.println(
        "Usage: alter table <table_name> [partition (partition_key = 'partition_value' [, ...])] compact [major|minor] "
        + "[-h recentHoursThresholdForPartialCompact (int type, only minor compact takes effect)] "
        + "[-f forceMode (When the compact time is less than the retention time, the compact time is automatically corrected to the table's retention time. When this is set, cancels this behavior)]");
  }
  private static final String MAJOR_COMPACT = "major_compact";
  private static final String MINOR_COMPACT = "minor_compact";

  private String taskName = "";

  public void run() throws OdpsException, ODPSConsoleException {
    ExecutionContext context = getContext();

    String tablePart = getCommandText();
    if (tablePart.toLowerCase().contains("partition")) {
      tablePart = tablePart.trim().split("\\s+")[0];
    }
    Coordinate coordinate = Coordinate.getCoordinateABC(tablePart);
    coordinate.interpretByCtx(context);

    DefaultOutputWriter writer = context.getOutputWriter();

    String projectName = coordinate.getProjectName();
    String schemaName = coordinate.getSchemaName();
    String tableName = coordinate.getObjectName();
    Table table = getCurrentOdps().tables().get(projectName, schemaName, tableName);

    if (!table.isTransactional()) {
      throw new OdpsException(tablePart + " is not a transactional table.");
    }
    int recentHours = -1;
    if (MINOR_COMPACT.equals(compactType) && params.containsKey("h")) {
      try {
        recentHours = Integer.parseInt(params.get("h"));
        int acidDataRetainHours = table.getAcidDataRetainHours();
        if (recentHours < acidDataRetainHours) {
          if (!params.containsKey("f")) {
            writer.writeError(
                "Warning: setting 'recentHoursThresholdForPartialCompact' below the data retention period ("
                + acidDataRetainHours + " hours) prevents past time travel. "
                + "It's now set to match the retention period. Use -f to override.");
            recentHours = acidDataRetainHours;
          }
        }
      } catch (NumberFormatException e) {
        throw new ODPSConsoleException(ODPSConsoleConstants.BAD_COMMAND
                                       + "recentHoursThresholdForPartialCompact should be Integer.");
      }
    }

    // do retry
    int retryTime = context.getRetryTimes();
    retryTime = retryTime > 0 ? retryTime : 1;
    while (retryTime > 0) {
      Task task = null;
      try {
        taskName = "console_merge_task_" + Calendar.getInstance().getTimeInMillis();
        task = new MergeTask(taskName, getCommandText());

        HashMap<String, String> taskConfig = QueryUtil.getTaskConfig();

        Map<String, String> settings = new HashMap<>();
        settings.put("odps.merge.txn.table.compact", compactType);
        settings.put("odps.merge.restructure.action", "hardlink");
        if (MINOR_COMPACT.equals(compactType)) {
          settings.put("odps.merge.txn.table.compact.txn.id", String.valueOf(recentHours));
        }
        addSetting(taskConfig, settings);

        for (Entry<String, String> property : taskConfig.entrySet()) {
          task.setProperty(property.getKey(), property.getValue());
        }

        runJob(task);
        // success
        break;
      } catch (UserInterruptException e) {
        throw e;
      } catch (Exception e) {
        retryTime--;
        if (retryTime == 0) {
          throw new ODPSConsoleException(e.getMessage());
        }
        writer.writeError("retry " + retryTime);
        writer.writeDebug(StringUtils.stringifyException(e));
      }
    }
    // no exception ,print success
    writer.writeError("OK");
  }

  public String getTaskName() {
    return taskName;
  }

  public String getInstanceId() {
    return instanceId;
  }

  public Map<String, String> getParams() {
    return params;
  }

  public CompactCommand(String command, String compactType, Map<String, String> params,
                        ExecutionContext context) {
    super(command, context);
    this.params = params;
    this.compactType = compactType;
  }

  private static String getCompactType(String type) {
    if (StringUtils.isNullOrEmpty(type)) {
      return null;
    }

    if (type.equalsIgnoreCase("major")) {
      return MAJOR_COMPACT;
    } else if (type.equalsIgnoreCase("minor")) {
      return MINOR_COMPACT;
    }

    return null;
  }

  public static CompactCommand parse(String commandString, ExecutionContext sessionContext)
      throws ODPSConsoleException {
    String regstr = "(?s)\\s*ALTER\\s+TABLE\\s+(.*)\\s+COMPACT\\s+(.*)";

    Pattern p = Pattern.compile(regstr, Pattern.CASE_INSENSITIVE);
    Matcher m = p.matcher(commandString);

    if (m.find()) {
      String tablePart = m.group(1).trim();
      String[] params = m.group(2).trim().split("\\s+");
      String compactType = getCompactType(params[0]);

      if (compactType == null) {
        throw new ODPSConsoleException(
            ODPSConsoleConstants.BAD_COMMAND + "Compact type should be MAJOR or MINOR.");
      }
      return new CompactCommand(tablePart, compactType,
                                CommandParserUtils.parseArguments(commandString), sessionContext);
    }
    return null;
  }
}
