package com.aliyun.openservices.odps.console.utils.jline;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.jline.builtins.Completers;
import org.jline.reader.Completer;
import org.jline.reader.EndOfFileException;
import org.jline.reader.Highlighter;
import org.jline.reader.History;
import org.jline.reader.LineReader;
import org.jline.reader.LineReader.Option;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.Parser;
import org.jline.reader.UserInterruptException;
import org.jline.reader.impl.completer.AggregateCompleter;
import org.jline.reader.impl.completer.StringsCompleter;
import org.jline.terminal.Terminal;
import org.jline.utils.AttributedStringBuilder;
import org.jline.utils.AttributedStyle;

import com.aliyun.odps.utils.StringUtils;
import com.aliyun.openservices.odps.console.utils.CommandParserUtils;
import com.aliyun.openservices.odps.console.utils.SignalUtil;

import sun.misc.SignalHandler;


/**
 * A wrapper of Jline line reader. This class is a singleton.
 *
 * When the operating system is windows, ODPSLineReader will not be initialized.
 */
public class ODPSLineReader {
  private LineReader lineReader;
  private LineReader confirmationReader;

  private static ODPSLineReader odpsLineReader = null;

  private ODPSLineReader() {
    LineReaderBuilder lineReaderBuilder = LineReaderBuilder.builder();
    setOptions(lineReaderBuilder);
    this.confirmationReader = lineReaderBuilder.build();
    setCompleter(lineReaderBuilder);
    setHighlighter(lineReaderBuilder);
    setHistory(lineReaderBuilder);
    setParser(lineReaderBuilder);
    this.lineReader = lineReaderBuilder.build();

    // save history on exit
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        (lineReader.getHistory()).save();
      } catch (IOException ignored) {
      }
    }));
  }

  public History getHistory() {
    return this.lineReader.getHistory();
  }

  public void registerSignalHandler(Terminal.Signal signal, Terminal.SignalHandler handler) {
    this.lineReader.getTerminal().handle(signal, handler);
  }

  public String readLine(String prompt, boolean isConfirmation) {
    return readLine(prompt, null, isConfirmation);
  }

  public String readLine(String prompt, Character mask) {
    return readLine(prompt, mask, false);
  }

  public String readLine(
      String prompt,
      Character mask,
      boolean isConfirmation) {
    try {
      if (!isConfirmation) {
        return lineReader.readLine(stylePrompt(prompt), mask);
      } else {
        return confirmationReader.readLine(prompt, mask);
      }
    } catch (UserInterruptException e) {
      if (StringUtils.isNullOrEmpty(e.getPartialLine())) {
        // Handle CTRL + C with no input, will quit
        return null;
      } else {
        // Clear current input
        return "";
      }
    } catch (EndOfFileException e) {
      // Handle CTRL + D, will quit
      return null;
    }
  }

  private String stylePrompt(String prompt) {
    return new AttributedStringBuilder()
        .style(
            new AttributedStyle()
                .foreground(AttributedStyle.GREEN)
                .bold()
        )
        .append(prompt)
        .toAnsi();
  }

  /**
   * Helper methods to build a reader
   */
  private void setParser(LineReaderBuilder readerBuilder) {
    Parser parser = new ODPSDefaultParser();
    readerBuilder.parser(parser);
  }

  private void setCompleter(LineReaderBuilder readerBuilder) {
    Completer completer = getCommandCompleter();
    readerBuilder.completer(completer);
  }

  private Completer getCommandCompleter() {
    List<Completer> customCompletor = new ArrayList<>();

    Set<String> candidateStrings = new HashSet<>();
    try {
      for (String key : CommandParserUtils.getAllCommandKeyWords()) {
        candidateStrings.add(key.toUpperCase());
        candidateStrings.add(key.toLowerCase());
      }
    } catch (AssertionError e) {
      return null;
    }

    if (!candidateStrings.isEmpty()) {
      // odps key word completer, use default whitespace for arg delimiter
      customCompletor.add(new StringsCompleter(candidateStrings));
    }

    // file patch completer, use default whitespace for arg delimiter
    customCompletor.add(new Completers.FileNameCompleter());

    // aggregate two argument comepletor
    return new AggregateCompleter(customCompletor);
  }

  private void setHighlighter(LineReaderBuilder readerBuilder) {
    Highlighter highlighter = new ODPSDefaultHighlighter();
    readerBuilder.highlighter(highlighter);
  }

  private void setOptions(LineReaderBuilder readerBuilder) {
    // disable the following events
    // See: https://www.gnu.org/software/bash/manual/html_node/Event-Designators.html
    readerBuilder.option(Option.DISABLE_EVENT_EXPANSION, true);
    readerBuilder.option(Option.CASE_INSENSITIVE_SEARCH, true);
    readerBuilder.option(Option.CASE_INSENSITIVE, true);
  }

  private void setHistory(LineReaderBuilder readerBuilder) {
    // TODO: make it an odps constant
    String historyfile = ".odpscmd.history";
    Path homeDir = Paths.get(System.getProperty("user.home"));

    if (Files.isSymbolicLink(homeDir)) {
        try {
            homeDir = homeDir.toRealPath();
        } catch (IOException e) {
            throw new RuntimeException("Can not get user.home real path");
        }
    }

    readerBuilder.variable(LineReader.HISTORY_FILE, homeDir.resolve(historyfile));
  }

  public static ODPSLineReader getInstance() {
    if (odpsLineReader == null) {
      odpsLineReader = new ODPSLineReader();
      SignalHandler defaultIntHandler = SignalUtil.getDefaultIntSignalHandler(Thread.currentThread());
      SignalUtil.registerSignalHandler("INT", defaultIntHandler);
    }

    return odpsLineReader;
  }
}
