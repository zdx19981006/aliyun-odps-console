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

package com.aliyun.openservices.odps.console.utils;

import com.aliyun.odps.utils.StringUtils;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.constants.ODPSConsoleConstants;

import java.util.ArrayList;
import java.util.List;

import static com.aliyun.openservices.odps.console.utils.RawStringCommandSplitter.State.*;

/**
 * Split a string into commands, and into tokens:
 * 1. remove comments(`--`, `#`)
 * 2. remove semicolon(';')
 * 3. ensure quoted string is closed
 *
 * multi-thread unsafe!
 */
public class RawStringCommandSplitter {

  public boolean getFindRawString() {
    return findRawString;
  }

  enum State {
    START,
    QUOTE,
    ESCAPE,
    PRE_RAW_STRING,
    RAW_STRING_DELIMITER,
    RAW_STRING,
    NORMAL,
    PRE_COMMENT,
    COMMENT,
    END
  }

  private final String input;
  private int i = 0;
  private State state;
  private Character quoteType;
  private final StringBuilder commandBuffer;
  private final ArrayList<String> commandResults;
  private final StringBuilder tokenBuffer;
  private final ArrayList<String> tokenResults;
  private boolean parsed;
  private final StringBuilder rawStringDelimiter;
  private RawStringCommandSplitter checkPoint;
  private boolean reset = false;
  private boolean findRawString = false;

  public RawStringCommandSplitter(String input) {
    this.input = input;
    this.state = START;
    this.quoteType = null;
    this.commandBuffer = new StringBuilder();
    this.commandResults = new ArrayList<>();
    this.tokenBuffer = new StringBuilder();
    this.tokenResults = new ArrayList<>();
    this.parsed = StringUtils.isNullOrEmpty(input);
    this.rawStringDelimiter = new StringBuilder();
  }

  private void flushBuffer(StringBuilder buffer, ArrayList<String> results, boolean isTrim) {
    if (buffer.length() > 0) {
      String s = buffer.toString();
      String t = s.trim();
      if (!t.isEmpty()) {
        if (isTrim) {
          results.add(t);
        } else {
          results.add(s);
        }
      }
      buffer.setLength(0);
    }
  }

  private void flushTokenBuffer() {
    flushBuffer(tokenBuffer, tokenResults, true);
  }

  private void flushCommandBuffer() {
    flushBuffer(commandBuffer, commandResults, false);
  }

  private void normalSwitch(char c) {
    switch(c) {
      case '"':
      case '\'':
        state = QUOTE;
        quoteType = c;
        commandBuffer.append(c);
        flushTokenBuffer();
        tokenBuffer.append(c);
        break;
      case '-':
        state = PRE_COMMENT;
        break;
      case ';':
        state = NORMAL;
        flushCommandBuffer();
        flushTokenBuffer();
        break;
      case '(':
      case ')':
        state = NORMAL;
        commandBuffer.append(c);
        flushTokenBuffer();
        tokenResults.add(String.valueOf(c));
        break;
      case ' ':
      case '\t':
      case '\f':
        state = NORMAL;
        commandBuffer.append(c);
        flushTokenBuffer();
        break;
      case '\r':
      case '\n':
        state = END;
        commandBuffer.append(c);
        flushTokenBuffer();
        break;
      case 'r':
      case 'R':
        if (reset) {
          reset = false;
          state = NORMAL;
        } else {
          saveState();
          state = PRE_RAW_STRING;
          flushTokenBuffer();
        }
        commandBuffer.append(c);
        tokenBuffer.append(c);
        break;
      default:
        state = NORMAL;
        commandBuffer.append(c);
        tokenBuffer.append(c);
    }
  }

  private void parse() throws ODPSConsoleException {
    for(; i < input.length(); i++) {
      char c = input.charAt(i);
      switch(state) {
        case START:
          switch(c) {
            case ' ':
            case '\t':
            case '\f':
            case '\n':
            case '\r':
              commandBuffer.append(c);
              break;
            case '#':
              state = COMMENT;
              break;
            default:
              normalSwitch(c);
          }
          break;
        case PRE_RAW_STRING:
          if (c == '"' || c == '\'') {
            quoteType = c;
            state = RAW_STRING_DELIMITER;
            commandBuffer.append(c);
            tokenBuffer.append(c);
          } else {
            reset = true;
            setState(checkPoint);
            i = i-1;
          }
          break;
        case RAW_STRING_DELIMITER:
          commandBuffer.append(c);
          tokenBuffer.append(c);
          if (c == '(') {
            state = RAW_STRING;
            flushTokenBuffer();
          } else {
            rawStringDelimiter.append(c);
          }
          if (i == input.length()-1) {
            reset = true;
            setState(checkPoint);
            i = i-1;
          }
          break;
        case RAW_STRING:
          String rawStringEnd = ")" + rawStringDelimiter.toString() + quoteType;
          int rawStringEndPosition = input.indexOf(rawStringEnd, i);
          if (rawStringEndPosition >= 0) {
            commandBuffer.append(input, i, rawStringEndPosition);
            tokenBuffer.append(input, i, rawStringEndPosition);
            flushTokenBuffer();
            tokenBuffer.append(rawStringEnd);
            commandBuffer.append(rawStringEnd);
            flushTokenBuffer();
            // plus ) and '
            i = rawStringEndPosition + rawStringDelimiter.length() + 1;
            state = NORMAL;
            findRawString = true;
          } else {
            reset = true;
            setState(checkPoint);
            i = i-1;
          }
          break;
        case PRE_COMMENT:
          if (c == '-') {
            state = COMMENT;
            flushTokenBuffer();
          } else {
            state = NORMAL;
            commandBuffer.append('-');
            tokenBuffer.append('-');
            i--;
          }
          break;
        case COMMENT:
          if (c == '\n' || c == '\r') {
            commandBuffer.append(c);
            state = END;
          }
          break;
        case END:
          if (c != '\n' && c != '\r') {
            state = START;
            i--;
          } else {
            commandBuffer.append(c);
          }
          break;
        case QUOTE:
          tokenBuffer.append(c);
          commandBuffer.append(c);
          if (c == quoteType) {
            state = NORMAL;
            flushTokenBuffer();
          } else if (c == '\\') {
            state = ESCAPE;
          }
          break;
        case ESCAPE:
          tokenBuffer.append(c);
          commandBuffer.append(c);
          state = QUOTE;
          break;
        case NORMAL:
          normalSwitch(c);
          break;
        default:
          throw new IllegalArgumentException(
              String.format("impossible thing happened. pos=%s, char='%s'", i, c));
      }
    }

    switch(state) {
      case QUOTE:
      case ESCAPE:
        throw new ODPSConsoleException(ODPSConsoleConstants.BAD_COMMAND + " string not closed");
      case PRE_RAW_STRING:
      case RAW_STRING:
      case RAW_STRING_DELIMITER:
        reset = true;
        setState(checkPoint);
        parse();
        return;
      default:
        // in case last token/command without ";"
        flushTokenBuffer();
        flushCommandBuffer();
    }

    parsed = true;
  }

  /**
   * get split commands (trimmed and without ';')
   * return empty list if input is null or empty
   */
  public List<String> getCommands() throws ODPSConsoleException {
    if (!parsed) {
      parse();
    }
    return commandResults;
  }

  /**
   * get split tokens (trimmed and without ';')
   * return empty list if input is null or empty
   */
  public List<String> getTokens() throws ODPSConsoleException {
    if (!parsed) {
      parse();
    }
    return tokenResults;
  }

  private void saveState() {
    RawStringCommandSplitter splitter = new RawStringCommandSplitter(input);
    splitter.setState(this);
    checkPoint = splitter;
  }

  private void setState(RawStringCommandSplitter splitter) {
    this.i = splitter.i;
    this.state = splitter.state;
    this.quoteType = splitter.quoteType;

    this.commandBuffer.setLength(0);
    this.commandBuffer.append(splitter.commandBuffer);

    this.commandResults.clear();
    this.commandResults.addAll(splitter.commandResults);

    this.tokenBuffer.setLength(0);
    this.tokenBuffer.append(splitter.tokenBuffer);

    this.tokenResults.clear();
    this.tokenResults.addAll(splitter.tokenResults);

    this.parsed = splitter.parsed;

    this.rawStringDelimiter.setLength(0);
    this.rawStringDelimiter.append(splitter.rawStringDelimiter);

    this.findRawString = splitter.findRawString;
  }


}
