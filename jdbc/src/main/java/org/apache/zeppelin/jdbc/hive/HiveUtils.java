/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.zeppelin.jdbc.hive;

import org.apache.commons.dbcp2.DelegatingStatement;
import org.apache.commons.lang3.StringUtils;
import org.apache.hive.common.util.HiveVersionInfo;
import org.apache.hive.jdbc.HiveStatement;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This class include hive specific stuff.
 * e.g. Display hive job execution info.
 *
 */
public class HiveUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(HiveUtils.class);
  private static final int DEFAULT_QUERY_PROGRESS_INTERVAL = 1000;

  private static final Pattern JOBURL_PATTERN =
          Pattern.compile(".*Tracking URL = (\\S*).*", Pattern.DOTALL);

  /**
   * Display hive job execution info, and progress info for hive >= 2.3
   *
   * @param stmt
   * @param context
   * @param displayLog
   */
  public static void startHiveMonitorThread(Statement stmt,
                                            InterpreterContext context,
                                            boolean displayLog) {
    HiveStatement hiveStmt = (HiveStatement)
            ((DelegatingStatement) ((DelegatingStatement) stmt).getDelegate()).getDelegate();
    String hiveVersion = HiveVersionInfo.getVersion();
    ProgressBar progressBarTemp = null;
    if (isProgressBarSupported(hiveVersion)) {
      LOGGER.debug("ProgressBar is supported for hive version: " + hiveVersion);
      progressBarTemp = new ProgressBar();
    } else {
      LOGGER.debug("ProgressBar is not supported for hive version: " + hiveVersion);
    }
    // need to use final variable progressBar in thread, so need progressBarTemp here.
    final ProgressBar progressBar = progressBarTemp;

    Thread thread = new Thread(() -> {
      while (hiveStmt.hasMoreLogs() && !Thread.interrupted()) {
        try {
          List<String> logs = hiveStmt.getQueryLog();
          String logsOutput = StringUtils.join(logs, System.lineSeparator());
          LOGGER.debug("Hive job output: " + logsOutput);
          boolean displayLogProperty = context.getBooleanLocalProperty("displayLog", displayLog);
          if (!StringUtils.isBlank(logsOutput) && displayLogProperty) {
            context.out.write(logsOutput + "\n");
            context.out.flush();
          }
          if (!StringUtils.isBlank(logsOutput) && progressBar != null && displayLogProperty) {
            progressBar.operationLogShowedToUser();
          }
          Optional<String> jobURL = extractJobURL(logsOutput);
          if (jobURL.isPresent()) {
            Map<String, String> infos = new HashMap<>();
            infos.put("jobUrl", jobURL.get());
            infos.put("label", "HIVE JOB");
            infos.put("tooltip", "View in YARN WEB UI");
            infos.put("noteId", context.getNoteId());
            infos.put("paraId", context.getParagraphId());
            context.getIntpEventClient().onParaInfosReceived(infos);
          }
          // refresh logs every 1 second.
          Thread.sleep(DEFAULT_QUERY_PROGRESS_INTERVAL);
        } catch (Exception e) {
          LOGGER.warn("Fail to write output", e);
        }
      }
      LOGGER.debug("Hive monitor thread is finished");
    });
    thread.setName("HiveMonitor-Thread");
    thread.setDaemon(true);
    thread.start();
    LOGGER.info("Start HiveMonitor-Thread for sql: " + stmt);

    if (progressBar != null) {
      // TODO(Luffy): 当Hive版本低于2.3时是不满足这个代码分支条件的，也不会触发new ProgressBar的初始化，
      //  但是为啥会去加载InPlaceUpdateStream，并且抛出了下列异常呢？（自己写的Demo一直没能复现）
      //  java.lang.NoClassDefFoundError: org/apache/hive/jdbc/logs/InPlaceUpdateStream,
      //  难道是因为hive-jdbc包是由SPI机制加载的缘故才导致了HiveStatement没调用的方法被提取加载和检查?
      //  但是我这边是由自定义Spi接口方式也没能复现，难道非得jdbc的Spi才有这个问题？
      //  jdbc spi也没能复现
      //  经过不断验证得出: 当progressBar.getInPlaceUpdateStream的返回值类型不是自定义的BeelineInPlaceUpdateStream类型，
      //  而是改成父接口InPlaceUpdateStream的话，就算progressBar.getInPlaceUpdateStream + hiveStmt.setInPlaceUpdateStream
      //  在低版本也不会ClassNotDefException,原因是由于方法返回值类型为接口的子类，而调用方使用的入参类型是父接口类型的话，
      //  就算方法调用不会走到这个代码分支，也会自动触发方法参数隐式转换成调用方需要的父接口类型（这里的InPlaceUpdateStream), 也就是要切记
      //  调用方入参类型为父类型，而获取参数的方法返回的是子类型，这种情况接口参数的自动转换会导致ClassNotDefException，哪怕代码分支执行条件
      //  不被满足也会自动触发这种转换, progressBar.setInPlaceUpdateStream是通过代理类将这种带自定方法参数隐式转换的逻辑放到桥梁类，不直接暴露
      //  给需要兼容高低版本的这个工具类，从而绕开这种自动转换导致的父接口class被动加载带来的ClassNotDefException问题
      hiveStmt.setInPlaceUpdateStream(progressBar.getInPlaceUpdateStream(context.out));

      //
    }
  }

  // Hive progress bar is supported from hive 2.3 (HIVE-16045)
  private static boolean isProgressBarSupported(String hiveVersion) {
    String[] tokens = hiveVersion.split("\\.");
    int majorVersion = Integer.parseInt(tokens[0]);
    int minorVersion = Integer.parseInt(tokens[1]);
    return majorVersion > 2 || ((majorVersion == 2) && minorVersion >= 3);
  }

  // extract hive job url from logs, it only works for MR engine.
  static Optional<String> extractJobURL(String log) {
    Matcher matcher = JOBURL_PATTERN.matcher(log);
    if (matcher.matches()) {
      String jobURL = matcher.group(1);
      return Optional.of(jobURL);
    }
    return Optional.empty();
  }
}
