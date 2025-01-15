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

import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Created by zhenhong.gzh on 16/3/17.
 */
public class CompactCommandTest {
  private String [] positive = {"alter\t table \rtbl_name    compact major \n ", "alter\t\r\n table tbl_name  partition (dt='s') compact minor"};

  private String [] negative = {"alter\t table \rtbl_name    compact \n ", "alter table tbl_name  partition (dt='1') compact mi ", "alter table tbl_name  compact mn"};

  private String [] withCompactId = {"alter\t table \rtbl_name    compact minor \n -h 1234 -f", "alter\t\r\n table tbl_name  partition (dt='s') compact minor -f 1234"};

  @Test
  public void test() throws Exception {
    CompactCommand compactCommand = null;
    ExecutionContext context = ExecutionContext.init();

    for (String cmd : positive) {
      compactCommand = CompactCommand.parse(cmd, context);
      assertNotNull(compactCommand);
    }

    int errorCount = 0;

    for (String cmd : negative) {

      try {
        compactCommand = CompactCommand.parse(cmd, context);
      } catch (ODPSConsoleException e) {
        ++ errorCount;
      }
    }

    Assert.assertEquals(errorCount, negative.length);
  }

  @Test
  public void testWithCompactId() throws Exception {
    CompactCommand compactCommand = null;
    ExecutionContext context = ExecutionContext.init();
    for (String cmd : withCompactId) {
      compactCommand = CompactCommand.parse(cmd, context);
      assertNotNull(compactCommand);
      assertTrue(compactCommand.getParams().containsKey("f"));
    }
  }
}
