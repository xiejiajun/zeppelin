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

package org.apache.zeppelin.interpreter.remote;

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.thrift.TException;
import org.apache.zeppelin.conf.ZeppelinConfiguration;
import org.apache.zeppelin.display.AngularObject;
import org.apache.zeppelin.display.AngularObjectRegistry;
import org.apache.zeppelin.display.GUI;
import org.apache.zeppelin.display.Input;
import org.apache.zeppelin.interpreter.ConfInterpreter;
import org.apache.zeppelin.interpreter.Interpreter;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterContextRunner;
import org.apache.zeppelin.interpreter.InterpreterException;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.interpreter.LifecycleManager;
import org.apache.zeppelin.interpreter.ManagedInterpreterGroup;
import org.apache.zeppelin.interpreter.thrift.InterpreterCompletion;
import org.apache.zeppelin.interpreter.thrift.RemoteInterpreterContext;
import org.apache.zeppelin.interpreter.thrift.RemoteInterpreterResult;
import org.apache.zeppelin.interpreter.thrift.RemoteInterpreterResultMessage;
import org.apache.zeppelin.interpreter.thrift.RemoteInterpreterService.Client;
import org.apache.zeppelin.scheduler.Job;
import org.apache.zeppelin.scheduler.RemoteScheduler;
import org.apache.zeppelin.scheduler.Scheduler;
import org.apache.zeppelin.scheduler.SchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Proxy for Interpreter instance that runs on separate process
 */
public class RemoteInterpreter extends Interpreter {
  private static final Logger LOGGER = LoggerFactory.getLogger(RemoteInterpreter.class);
  private static final Gson gson = new Gson();


  private String className;
  private String sessionId;
  private FormType formType;

  private RemoteInterpreterProcess interpreterProcess;
  private volatile boolean isOpened = false;
  private volatile boolean isCreated = false;

  private LifecycleManager lifecycleManager;

  /**
   * Remote interpreter and manage interpreter process
   */
  public RemoteInterpreter(Properties properties,
                           String sessionId,
                           String className,
                           String userName,
                           LifecycleManager lifecycleManager) {
    super(properties);
    this.sessionId = sessionId;
    this.className = className;
    this.setUserName(userName);
    this.lifecycleManager = lifecycleManager;
  }

  public boolean isOpened() {
    return isOpened;
  }

  @VisibleForTesting
  public void setOpened(boolean opened) {
    isOpened = opened;
  }

  @Override
  public String getClassName() {
    return className;
  }

  public String getSessionId() {
    return this.sessionId;
  }

  public synchronized RemoteInterpreterProcess getOrCreateInterpreterProcess() throws IOException {
    if (this.interpreterProcess != null) {
      return this.interpreterProcess;
    }
    ManagedInterpreterGroup intpGroup = getInterpreterGroup();
    // TODO 解释器启动进程: 由于这里的properties是直接引用成员变量，没有调用父亲类的getProperties()方法进行过替换，
    //  所以spark 解释器cluster模式没法绑定用户名到yarn application,因为cluster模式的Driver(同时也是AM)一起来就运行到yarn上面了，
    //  这时候其实Driver里面运行的还不是SparkInterpreter，而是RemoteInterpreterServer,所以用户名宏变量还没被替换,就保持
    //  原来的spark.app.name把AM运行起来了；再来看Client模式，一开始Driver里面运行的还是RemoteInterpreterServer,因为Yarn Client
    //  模式下Driver是先启动在本地，然后再向RM申请容器启动AM的，所以最终是RemoteInterpreterServer运行在本地，然后再启动Spark解释器时
    //  替换掉spark.app.name中的宏变量再在yarn中启动spark解释器

    this.interpreterProcess = intpGroup.getOrCreateInterpreterProcess(getUserName(), properties);
    return interpreterProcess;
  }

  public ManagedInterpreterGroup getInterpreterGroup() {
    return (ManagedInterpreterGroup) super.getInterpreterGroup();
  }

  @Override
  public void open() throws InterpreterException {
    synchronized (this) {
      if (!isOpened) {
        // create all the interpreters of the same session first, then Open the internal interpreter
        // of this RemoteInterpreter.
        // The why we we create all the interpreter of the session is because some interpreter
        // depends on other interpreter. e.g. PySparkInterpreter depends on SparkInterpreter.
        // also see method Interpreter.getInterpreterInTheSameSessionByClassName
        for (Interpreter interpreter : getInterpreterGroup()
                                        .getOrCreateSession(this.getUserName(), sessionId)) {
          try {
            if (!(interpreter instanceof ConfInterpreter)) {
              // TODO 启动解释器进程入口
              ((RemoteInterpreter) interpreter).internal_create();
            }
          } catch (IOException e) {
            throw new InterpreterException(e);
          }
        }

        interpreterProcess.callRemoteFunction(new RemoteInterpreterProcess.RemoteFunction<Void>() {
          @Override
          public Void call(Client client) throws Exception {
            /**
             * TODO 看这段日志，无需跑起来debug就能知道RemoteInterpreter是用于连接各个具体解释器的RPC代理：
             *  INFO [2020-02-05 13:19:30,857] ({pool-2-thread-24} ManagedInterpreterGroup.java[getOrCreateInterpreterProcess]:61) - Create InterpreterProcess for InterpreterGroup: sh:shared_process
             *  INFO [2020-02-05 13:19:30,857] ({pool-2-thread-24} ShellScriptLauncher.java[launch]:48) - Launching Interpreter: sh
             *  INFO [2020-02-05 13:19:30,859] ({pool-2-thread-24} RemoteInterpreterManagedProcess.java[start]:115) - Thrift server for callback will start. Port: 43637
             *  INFO [2020-02-05 13:19:31,360] ({pool-2-thread-24} RemoteInterpreterManagedProcess.java[start]:190) - Run interpreter process [/usr/lib/zeppelin/bin/interpreter.sh, -d, /usr/lib/zeppelin/interpreter/sh, -c, 172.31.31.13, -p, 43637, -r, :, -l, /usr/lib/zeppelin/local-repo/sh, -g, sh]
             *  INFO [2020-02-05 13:19:32,839] ({pool-18-thread-1} RemoteInterpreterManagedProcess.java[callback]:123) - RemoteInterpreterServer Registered: CallbackInfo(host:172.31.31.13, port:35431)
             *  INFO [2020-02-05 13:19:32,839] ({pool-2-thread-24} TimeoutLifecycleManager.java[onInterpreterProcessStarted]:68) - Process of InterpreterGroup sh:shared_process is started
             *  INFO [2020-02-05 13:19:32,841] ({pool-2-thread-24} RemoteInterpreter.java[call]:172) - Create RemoteInterpreter org.apache.zeppelin.shell.ShellInterpreter
             *  INFO [2020-02-05 13:19:32,951] ({pool-2-thread-24} RemoteInterpreter.java[call]:146) - Open RemoteInterpreter org.apache.zeppelin.shell.ShellInterpreter
             *  INFO [2020-02-05 13:19:32,951] ({pool-2-thread-24} RemoteInterpreter.java[pushAngularObjectRegistryToRemote]:449) - Push local angular object registry from ZeppelinServer to remote interpreter group sh:shared_process
             *  INFO [2020-02-05 13:19:34,365] ({pool-2-thread-24} NotebookServer.java[afterStatusChange]:2314) - Job 20200205-074503_1029817251 is finished successfully, status: FINISHED
             */

              // TODO 而且可以根据下面这行日志判断执行某个段落的是哪个解释器:这个地方对于通过日志查询执行指定段落用到的解释器非常有用，找到对应解释器后就方便定位Zeppelin服务的问题了
              //  例如：RemoteInterpreter.java[call]:146) - Open RemoteInterpreter org.apache.zeppelin.spark.DepInterpreter
              //       根据上面这行日志，定位到执行%spark.dep段落的是DepInterpreter(0.8分支有该解释器，新分支还未细跟换成了哪个)

            LOGGER.info("Open RemoteInterpreter {}", getClassName());
            // open interpreter here instead of in the jobRun method in RemoteInterpreterServer
            // client.open(sessionId, className);
            // Push angular object loaded from JSON file to remote interpreter
            synchronized (getInterpreterGroup()) {
              if (!getInterpreterGroup().isAngularRegistryPushed()) {
                pushAngularObjectRegistryToRemote(client);
                getInterpreterGroup().setAngularRegistryPushed(true);
              }
            }
            return null;
          }
        });
        isOpened = true;
        this.lifecycleManager.onInterpreterUse(this.getInterpreterGroup(), sessionId);
      }
    }
  }

  private void internal_create() throws IOException {
    synchronized (this) {
      if (!isCreated) {
        // TODO 创建解释器进程（入口是RemoteInterpreterServer)
        this.interpreterProcess = getOrCreateInterpreterProcess();
        interpreterProcess.callRemoteFunction(new RemoteInterpreterProcess.RemoteFunction<Void>() {
          @Override
          public Void call(Client client) throws Exception {
            LOGGER.info("Create RemoteInterpreter {}", getClassName());
            // TODO 使用Thrift客户端和解释器进程交互，加载对应的解释器逻辑实现类
            client.createInterpreter(getInterpreterGroup().getId(), sessionId,
                className, (Map) getProperties(), getUserName());
            return null;
          }
        });
        isCreated = true;
      }
    }
  }


  /**
   * TODO 用于向远程RemoteInterpreterServer启动的解释器服务发送close请求，对应解释器收到请求后再调用自己的close方法kill -2杀掉进程
   * @throws InterpreterException
   */
  @Override
  public void close() throws InterpreterException {
    if (isOpened) {
      RemoteInterpreterProcess interpreterProcess = null;
      try {
        interpreterProcess = getOrCreateInterpreterProcess();
      } catch (IOException e) {
        throw new InterpreterException(e);
      }
      interpreterProcess.callRemoteFunction(new RemoteInterpreterProcess.RemoteFunction<Void>() {
        @Override
        public Void call(Client client) throws Exception {
          client.close(sessionId, className);
          return null;
        }
      });
      isOpened = false;
      this.lifecycleManager.onInterpreterUse(this.getInterpreterGroup(), sessionId);
    } else {
      LOGGER.warn("close is called when RemoterInterpreter is not opened for " + className);
    }
  }

  @Override
  public InterpreterResult interpret(final String st, final InterpreterContext context)
      throws InterpreterException {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("st:\n{}", st);
    }

    final FormType form = getFormType();
    RemoteInterpreterProcess interpreterProcess = null;
    try {
      interpreterProcess = getOrCreateInterpreterProcess();
    } catch (IOException e) {
      throw new InterpreterException(e);
    }
    InterpreterContextRunnerPool interpreterContextRunnerPool = interpreterProcess
        .getInterpreterContextRunnerPool();
    List<InterpreterContextRunner> runners = context.getRunners();
    if (runners != null && runners.size() != 0) {
      // assume all runners in this InterpreterContext have the same note id
      String noteId = runners.get(0).getNoteId();

      interpreterContextRunnerPool.clear(noteId);
      interpreterContextRunnerPool.addAll(noteId, runners);
    }
    this.lifecycleManager.onInterpreterUse(this.getInterpreterGroup(), sessionId);
    return interpreterProcess.callRemoteFunction(
        new RemoteInterpreterProcess.RemoteFunction<InterpreterResult>() {
          @Override
          public InterpreterResult call(Client client) throws Exception {

            RemoteInterpreterResult remoteResult = client.interpret(
                sessionId, className, st, convert(context));
            Map<String, Object> remoteConfig = (Map<String, Object>) gson.fromJson(
                remoteResult.getConfig(), new TypeToken<Map<String, Object>>() {
                }.getType());
            context.getConfig().clear();
            context.getConfig().putAll(remoteConfig);
            GUI currentGUI = context.getGui();
            GUI currentNoteGUI = context.getNoteGui();
            if (form == FormType.NATIVE) {
              GUI remoteGui = GUI.fromJson(remoteResult.getGui());
              GUI remoteNoteGui = GUI.fromJson(remoteResult.getNoteGui());
              currentGUI.clear();
              currentGUI.setParams(remoteGui.getParams());
              currentGUI.setForms(remoteGui.getForms());
              currentNoteGUI.setParams(remoteNoteGui.getParams());
              currentNoteGUI.setForms(remoteNoteGui.getForms());
            } else if (form == FormType.SIMPLE) {
              final Map<String, Input> currentForms = currentGUI.getForms();
              final Map<String, Object> currentParams = currentGUI.getParams();
              final GUI remoteGUI = GUI.fromJson(remoteResult.getGui());
              final Map<String, Input> remoteForms = remoteGUI.getForms();
              final Map<String, Object> remoteParams = remoteGUI.getParams();
              currentForms.putAll(remoteForms);
              currentParams.putAll(remoteParams);
            }

            InterpreterResult result = convert(remoteResult);
            return result;
          }
        }
    );

  }

  @Override
  public void cancel(final InterpreterContext context) throws InterpreterException {
    if (!isOpened) {
      LOGGER.warn("Cancel is called when RemoterInterpreter is not opened for " + className);
      return;
    }
    RemoteInterpreterProcess interpreterProcess = null;
    try {
      interpreterProcess = getOrCreateInterpreterProcess();
    } catch (IOException e) {
      throw new InterpreterException(e);
    }
    this.lifecycleManager.onInterpreterUse(this.getInterpreterGroup(), sessionId);
    interpreterProcess.callRemoteFunction(new RemoteInterpreterProcess.RemoteFunction<Void>() {
      @Override
      public Void call(Client client) throws Exception {
        client.cancel(sessionId, className, convert(context));
        return null;
      }
    });
  }

  @Override
  public FormType getFormType() throws InterpreterException {
    if (formType != null) {
      return formType;
    }

    // it is possible to call getFormType before it is opened
    synchronized (this) {
      if (!isOpened) {
        open();
      }
    }
    RemoteInterpreterProcess interpreterProcess = null;
    try {
      interpreterProcess = getOrCreateInterpreterProcess();
    } catch (IOException e) {
      throw new InterpreterException(e);
    }
    this.lifecycleManager.onInterpreterUse(this.getInterpreterGroup(), sessionId);
    FormType type = interpreterProcess.callRemoteFunction(
        new RemoteInterpreterProcess.RemoteFunction<FormType>() {
          @Override
          public FormType call(Client client) throws Exception {
            formType = FormType.valueOf(client.getFormType(sessionId, className));
            return formType;
          }
        });
    return type;
  }


  @Override
  public int getProgress(final InterpreterContext context) throws InterpreterException {
    if (!isOpened) {
      LOGGER.warn("getProgress is called when RemoterInterpreter is not opened for " + className);
      return 0;
    }
    RemoteInterpreterProcess interpreterProcess = null;
    try {
      interpreterProcess = getOrCreateInterpreterProcess();
    } catch (IOException e) {
      throw new InterpreterException(e);
    }
    this.lifecycleManager.onInterpreterUse(this.getInterpreterGroup(), sessionId);
    return interpreterProcess.callRemoteFunction(
        new RemoteInterpreterProcess.RemoteFunction<Integer>() {
          @Override
          public Integer call(Client client) throws Exception {
            return client.getProgress(sessionId, className, convert(context));
          }
        });
  }


  @Override
  public List<InterpreterCompletion> completion(final String buf, final int cursor,
                                                final InterpreterContext interpreterContext)
      throws InterpreterException {
    if (!isOpened) {
      open();
    }
    RemoteInterpreterProcess interpreterProcess = null;
    try {
      interpreterProcess = getOrCreateInterpreterProcess();
    } catch (IOException e) {
      throw new InterpreterException(e);
    }
    this.lifecycleManager.onInterpreterUse(this.getInterpreterGroup(), sessionId);
    return interpreterProcess.callRemoteFunction(
        new RemoteInterpreterProcess.RemoteFunction<List<InterpreterCompletion>>() {
          @Override
          public List<InterpreterCompletion> call(Client client) throws Exception {
            return client.completion(sessionId, className, buf, cursor,
                convert(interpreterContext));
          }
        });
  }

  public String getStatus(final String jobId) {
    if (!isOpened) {
      LOGGER.warn("getStatus is called when RemoteInterpreter is not opened for " + className);
      return Job.Status.UNKNOWN.name();
    }
    RemoteInterpreterProcess interpreterProcess = null;
    try {
      interpreterProcess = getOrCreateInterpreterProcess();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    this.lifecycleManager.onInterpreterUse(this.getInterpreterGroup(), sessionId);
    return interpreterProcess.callRemoteFunction(
        new RemoteInterpreterProcess.RemoteFunction<String>() {
          @Override
          public String call(Client client) throws Exception {
            return client.getStatus(sessionId, jobId);
          }
        });
  }


  @Override
  public Scheduler getScheduler() {
    int maxConcurrency = Integer.parseInt(
        getProperty("zeppelin.interpreter.max.poolsize",
            ZeppelinConfiguration.ConfVars.ZEPPELIN_INTERPRETER_MAX_POOL_SIZE.getIntValue() + ""));
    // one session own one Scheduler, so that when one session is closed, all the jobs/paragraphs
    // running under the scheduler of this session will be aborted.
    Scheduler s = new RemoteScheduler(
        RemoteInterpreter.class.getName() + "-" + getInterpreterGroup().getId() + "-" + sessionId,
        SchedulerFactory.singleton().getExecutor(),
        sessionId,
        this,
        SchedulerFactory.singleton(),
        maxConcurrency);
    return SchedulerFactory.singleton().createOrGetScheduler(s);
  }

  private RemoteInterpreterContext convert(InterpreterContext ic) {
    return new RemoteInterpreterContext(ic.getNoteId(), ic.getParagraphId(), ic.getReplName(),
        ic.getParagraphTitle(), ic.getParagraphText(), gson.toJson(ic.getAuthenticationInfo()),
        gson.toJson(ic.getConfig()), ic.getGui().toJson(), gson.toJson(ic.getNoteGui()),
        gson.toJson(ic.getRunners()));
  }

  private InterpreterResult convert(RemoteInterpreterResult result) {
    InterpreterResult r = new InterpreterResult(
        InterpreterResult.Code.valueOf(result.getCode()));

    for (RemoteInterpreterResultMessage m : result.getMsg()) {
      r.add(InterpreterResult.Type.valueOf(m.getType()), m.getData());
    }

    return r;
  }

  /**
   * Push local angular object registry to
   * remote interpreter. This method should be
   * call ONLY once when the first Interpreter is created
   */
  private void pushAngularObjectRegistryToRemote(Client client) throws TException {
    final AngularObjectRegistry angularObjectRegistry = this.getInterpreterGroup()
        .getAngularObjectRegistry();
    if (angularObjectRegistry != null && angularObjectRegistry.getRegistry() != null) {
      final Map<String, Map<String, AngularObject>> registry = angularObjectRegistry
          .getRegistry();
      LOGGER.info("Push local angular object registry from ZeppelinServer to" +
          " remote interpreter group {}", this.getInterpreterGroup().getId());
      final java.lang.reflect.Type registryType = new TypeToken<Map<String,
          Map<String, AngularObject>>>() {
      }.getType();
      client.angularRegistryPush(gson.toJson(registry, registryType));
    }
  }

  @Override
  public String toString() {
    return "RemoteInterpreter_" + className + "_" + sessionId;
  }
}
