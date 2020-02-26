/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.zeppelin.interpreter;

import org.apache.zeppelin.interpreter.remote.RemoteInterpreterProcess;
import org.apache.zeppelin.scheduler.Job;
import org.apache.zeppelin.scheduler.Scheduler;
import org.apache.zeppelin.scheduler.SchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

/**
 * ManagedInterpreterGroup runs under zeppelin server
 */
public class ManagedInterpreterGroup extends InterpreterGroup {

  private static final Logger LOGGER = LoggerFactory.getLogger(ManagedInterpreterGroup.class);

  private InterpreterSetting interpreterSetting;
  private RemoteInterpreterProcess remoteInterpreterProcess; // attached remote interpreter process

  /**
   * Create InterpreterGroup with given id and interpreterSetting, used in ZeppelinServer
   * @param id
   * @param interpreterSetting
   */
  ManagedInterpreterGroup(String id, InterpreterSetting interpreterSetting) {
    super(id);
    this.interpreterSetting = interpreterSetting;
  }

  public InterpreterSetting getInterpreterSetting() {
    return interpreterSetting;
  }

  /**
   * TODO 获取RemoteInterpreterProcess（这里面保存了连接远程解释器服务的相关参数），没有就创建（即启动解释器服务）
   * @param userName
   * @param properties
   * @return
   * @throws IOException
   */
  public synchronized RemoteInterpreterProcess getOrCreateInterpreterProcess(String userName,
                                                                             Properties properties)
      throws IOException {
    if (remoteInterpreterProcess == null) {
      LOGGER.info("Create InterpreterProcess for InterpreterGroup: " + getId());
      remoteInterpreterProcess = interpreterSetting.createInterpreterProcess(id, userName,
          properties);
      // TODO 启动解释器
      remoteInterpreterProcess.start(userName);
      interpreterSetting.getLifecycleManager().onInterpreterProcessStarted(this);
      remoteInterpreterProcess.getRemoteInterpreterEventPoller()
          .setInterpreterProcess(remoteInterpreterProcess);
      remoteInterpreterProcess.getRemoteInterpreterEventPoller().setInterpreterGroup(this);
      remoteInterpreterProcess.getRemoteInterpreterEventPoller().start();
      getInterpreterSetting().getRecoveryStorage()
          .onInterpreterClientStart(remoteInterpreterProcess);
    }
    return remoteInterpreterProcess;
  }

  public RemoteInterpreterProcess getInterpreterProcess() {
    return remoteInterpreterProcess;
  }

  public RemoteInterpreterProcess getRemoteInterpreterProcess() {
    return remoteInterpreterProcess;
  }


  /**
   * Close all interpreter instances in this group
   */
  public synchronized void close() {
    LOGGER.info("Close InterpreterGroup: " + id);
    for (String sessionId : sessions.keySet()) {
      close(sessionId);
    }
  }

  /**
   * TODO 关闭当前session的所有解释器实例: per user模式其实一个sessionId只对应一个Interpreter
   * Close all interpreter instances in this session
   * @param sessionId
   */
  public synchronized void close(String sessionId) {
    LOGGER.info("Close Session: " + sessionId + " for interpreter setting: " +
        interpreterSetting.getName());
    // TODO 关闭所有解释器实例
    close(sessions.remove(sessionId));
    //TODO(zjffdu) whether close InterpreterGroup if there's no session left in Zeppelin Server
    if (sessions.isEmpty() && interpreterSetting != null) {
      LOGGER.info("Remove this InterpreterGroup: {} as all the sessions are closed", id);
      interpreterSetting.removeInterpreterGroup(id);
      if (remoteInterpreterProcess != null) {
        LOGGER.info("Kill RemoteInterpreterProcess");
        remoteInterpreterProcess.stop();
        try {
          interpreterSetting.getRecoveryStorage().onInterpreterClientStop(remoteInterpreterProcess);
        } catch (IOException e) {
          LOGGER.error("Fail to store recovery data", e);
        }
        remoteInterpreterProcess = null;
      }
    }
  }

  /**
   * TODO 关闭所有解释器实例
   * @param interpreters
   */
  private void close(Collection<Interpreter> interpreters) {
    if (interpreters == null) {
      return;
    }

    for (Interpreter interpreter : interpreters) {
      Scheduler scheduler = interpreter.getScheduler();
      // TODO 先终止所有Job
      for (Job job : scheduler.getJobsRunning()) {
        job.abort();
        job.setStatus(Job.Status.ABORT);
        LOGGER.info("Job " + job.getJobName() + " aborted ");
      }
      for (Job job : scheduler.getJobsWaiting()) {
        job.abort();
        job.setStatus(Job.Status.ABORT);
        LOGGER.info("Job " + job.getJobName() + " aborted ");
      }

      try {
        // TODO 关闭解释器：通过RemoteInterpreter RPC客户端发送请求调用各个解释器自己实现的close方法
        interpreter.close();
      } catch (InterpreterException e) {
        LOGGER.warn("Fail to close interpreter " + interpreter.getClassName(), e);
      }
      //TODO(zjffdu) move the close of schedule to Interpreter
      if (null != scheduler) {
        SchedulerFactory.singleton().removeScheduler(scheduler.getName());
      }
    }
  }

  public synchronized List<Interpreter> getOrCreateSession(String user, String sessionId) {
    if (sessions.containsKey(sessionId)) {
      return sessions.get(sessionId);
    } else {
      List<Interpreter> interpreters = interpreterSetting.createInterpreters(user, id, sessionId);
      for (Interpreter interpreter : interpreters) {
        interpreter.setInterpreterGroup(this);
      }
      LOGGER.info("Create Session: {} in InterpreterGroup: {} for user: {}", sessionId, id, user);
      sessions.put(sessionId, interpreters);
      return interpreters;
    }
  }

}
