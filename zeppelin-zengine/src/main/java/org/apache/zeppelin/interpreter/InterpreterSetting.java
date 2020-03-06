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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.annotations.SerializedName;
import com.google.gson.internal.StringMap;
import org.apache.commons.io.FileUtils;
import org.apache.zeppelin.conf.ZeppelinConfiguration;
import org.apache.zeppelin.dep.Dependency;
import org.apache.zeppelin.dep.DependencyResolver;
import org.apache.zeppelin.display.AngularObjectRegistry;
import org.apache.zeppelin.display.AngularObjectRegistryListener;
import org.apache.zeppelin.helium.ApplicationEventListener;
import org.apache.zeppelin.interpreter.launcher.InterpreterLaunchContext;
import org.apache.zeppelin.interpreter.launcher.InterpreterLauncher;
import org.apache.zeppelin.interpreter.launcher.ShellScriptLauncher;
import org.apache.zeppelin.interpreter.launcher.SparkInterpreterLauncher;
import org.apache.zeppelin.interpreter.lifecycle.NullLifecycleManager;
import org.apache.zeppelin.interpreter.recovery.NullRecoveryStorage;
import org.apache.zeppelin.interpreter.recovery.RecoveryStorage;
import org.apache.zeppelin.interpreter.remote.RemoteAngularObjectRegistry;
import org.apache.zeppelin.interpreter.remote.RemoteInterpreter;
import org.apache.zeppelin.interpreter.remote.RemoteInterpreterEventPoller;
import org.apache.zeppelin.interpreter.remote.RemoteInterpreterProcess;
import org.apache.zeppelin.interpreter.remote.RemoteInterpreterProcessListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.zeppelin.conf.ZeppelinConfiguration.ConfVars.ZEPPELIN_INTERPRETER_MAX_POOL_SIZE;
import static org.apache.zeppelin.conf.ZeppelinConfiguration.ConfVars.ZEPPELIN_INTERPRETER_OUTPUT_LIMIT;
import static org.apache.zeppelin.util.IdHashes.generateId;

/**
 * Represent one InterpreterSetting in the interpreter setting page
 */
public class InterpreterSetting {

  private static final Logger LOGGER = LoggerFactory.getLogger(InterpreterSetting.class);
  private static final String SHARED_PROCESS = "shared_process";
  private static final String SHARED_SESSION = "shared_session";
  private static final Map<String, Object> DEFAULT_EDITOR = ImmutableMap.of(
      "language", (Object) "text",
      "editOnDblClick", false);

  private String id;
  private String name;
  // the original interpreter setting template name where it is created from
  private String group;

  //TODO(zjffdu) make the interpreter.json consistent with interpreter-setting.json
  /**
   * properties can be either Properties or Map<String, InterpreterProperty>
   * properties should be:
   * - Properties when Interpreter instances are saved to `conf/interpreter.json` file
   * - Map<String, InterpreterProperty> when Interpreters are registered
   * : this is needed after https://github.com/apache/zeppelin/pull/1145
   * which changed the way of getting default interpreter setting AKA interpreterSettingsRef
   * Note(mina): In order to simplify the implementation, I chose to change properties
   * from Properties to Object instead of creating new classes.
   */
  private Object properties = new Properties();

  private Status status;
  private String errorReason;

  @SerializedName("interpreterGroup")
  private List<InterpreterInfo> interpreterInfos;

  private List<Dependency> dependencies = new ArrayList<>();
  private InterpreterOption option = new InterpreterOption();

  @SerializedName("runner")
  private InterpreterRunner interpreterRunner;

  ///////////////////////////////////////////////////////////////////////////////////////////
  private transient InterpreterSettingManager interpreterSettingManager;
  private transient String interpreterDir;
  private final transient Map<String, ManagedInterpreterGroup> interpreterGroups =
      new ConcurrentHashMap<>();

  private final transient ReentrantReadWriteLock.ReadLock interpreterGroupReadLock;
  private final transient ReentrantReadWriteLock.WriteLock interpreterGroupWriteLock;

  private transient AngularObjectRegistryListener angularObjectRegistryListener;
  private transient RemoteInterpreterProcessListener remoteInterpreterProcessListener;
  private transient ApplicationEventListener appEventListener;
  private transient DependencyResolver dependencyResolver;

  private transient Map<String, String> infos;

  // Map of the note and paragraphs which has runtime infos generated by this interpreter setting.
  // This map is used to clear the infos in paragraph when the interpretersetting is restarted
  private transient Map<String, Set<String>> runtimeInfosToBeCleared;

  private transient ZeppelinConfiguration conf = new ZeppelinConfiguration();

  // TODO(zjffdu) ShellScriptLauncher is the only launcher implemention for now. It could be other
  // launcher in future when we have other launcher implementation. e.g. third party launcher
  // service like livy
  private transient InterpreterLauncher launcher;

  private transient LifecycleManager lifecycleManager;
  ///////////////////////////////////////////////////////////////////////////////////////////



  private transient RecoveryStorage recoveryStorage;
  ///////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Builder class for InterpreterSetting
   */
  public static class Builder {
    private InterpreterSetting interpreterSetting;

    public Builder() {
      this.interpreterSetting = new InterpreterSetting();
    }

    public Builder setId(String id) {
      interpreterSetting.id = id;
      return this;
    }

    public Builder setName(String name) {
      interpreterSetting.name = name;
      return this;
    }

    public Builder setGroup(String group) {
      interpreterSetting.group = group;
      return this;
    }

    public Builder setInterpreterInfos(List<InterpreterInfo> interpreterInfos) {
      interpreterSetting.interpreterInfos = interpreterInfos;
      return this;
    }

    public Builder setProperties(Object properties) {
      interpreterSetting.properties = properties;
      return this;
    }

    public Builder setOption(InterpreterOption option) {
      interpreterSetting.option = option;
      return this;
    }

    public Builder setInterpreterDir(String interpreterDir) {
      interpreterSetting.interpreterDir = interpreterDir;
      return this;
    }

    public Builder setRunner(InterpreterRunner runner) {
      interpreterSetting.interpreterRunner = runner;
      return this;
    }

    public Builder setDependencies(List<Dependency> dependencies) {
      interpreterSetting.dependencies = dependencies;
      return this;
    }

    public Builder setConf(ZeppelinConfiguration conf) {
      interpreterSetting.conf = conf;
      return this;
    }

    public Builder setDependencyResolver(DependencyResolver dependencyResolver) {
      interpreterSetting.dependencyResolver = dependencyResolver;
      return this;
    }

    public Builder setInterpreterRunner(InterpreterRunner runner) {
      interpreterSetting.interpreterRunner = runner;
      return this;
    }

    public Builder setIntepreterSettingManager(
        InterpreterSettingManager interpreterSettingManager) {
      interpreterSetting.interpreterSettingManager = interpreterSettingManager;
      return this;
    }

    public Builder setRemoteInterpreterProcessListener(RemoteInterpreterProcessListener
                                                       remoteInterpreterProcessListener) {
      interpreterSetting.remoteInterpreterProcessListener = remoteInterpreterProcessListener;
      return this;
    }

    public Builder setAngularObjectRegistryListener(
        AngularObjectRegistryListener angularObjectRegistryListener) {
      interpreterSetting.angularObjectRegistryListener = angularObjectRegistryListener;
      return this;
    }

    public Builder setApplicationEventListener(ApplicationEventListener applicationEventListener) {
      interpreterSetting.appEventListener = applicationEventListener;
      return this;
    }

    public Builder setLifecycleManager(LifecycleManager lifecycleManager) {
      interpreterSetting.lifecycleManager = lifecycleManager;
      return this;
    }

    public Builder setRecoveryStorage(RecoveryStorage recoveryStorage) {
      interpreterSetting.recoveryStorage = recoveryStorage;
      return this;
    }

    public InterpreterSetting create() {
      // post processing
      interpreterSetting.postProcessing();
      return interpreterSetting;
    }
  }

  public InterpreterSetting() {
    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    this.id = generateId();
    interpreterGroupReadLock = lock.readLock();
    interpreterGroupWriteLock = lock.writeLock();
  }

  void postProcessing() {
    this.status = Status.READY;
    this.id = this.name;
    if (this.lifecycleManager == null) {
      this.lifecycleManager = new NullLifecycleManager(conf);
    }
    if (this.recoveryStorage == null) {
      try {
        this.recoveryStorage = new NullRecoveryStorage(conf, interpreterSettingManager);
      } catch (IOException e) {
        // ignore this exception as NullRecoveryStorage will do nothing.
      }
    }
  }

  /**
   * Create interpreter from InterpreterSettingTemplate
   *
   * @param o interpreterSetting from InterpreterSettingTemplate
   */
  public InterpreterSetting(InterpreterSetting o) {
    this();
    this.id = o.name;
    this.name = o.name;
    this.group = o.group;
    this.properties = convertInterpreterProperties(
        (Map<String, DefaultInterpreterProperty>) o.getProperties());
    this.interpreterInfos = new ArrayList<>(o.getInterpreterInfos());
    this.option = InterpreterOption.fromInterpreterOption(o.getOption());
    this.dependencies = new ArrayList<>(o.getDependencies());
    this.interpreterDir = o.getInterpreterDir();
    this.interpreterRunner = o.getInterpreterRunner();
    this.conf = o.getConf();
  }

  private void createLauncher() {
    if (group.equals("spark")) {
      this.launcher = new SparkInterpreterLauncher(this.conf, this.recoveryStorage);
    } else {
      this.launcher = new ShellScriptLauncher(this.conf, this.recoveryStorage);
    }
  }

  public AngularObjectRegistryListener getAngularObjectRegistryListener() {
    return angularObjectRegistryListener;
  }

  public RemoteInterpreterProcessListener getRemoteInterpreterProcessListener() {
    return remoteInterpreterProcessListener;
  }

  public ApplicationEventListener getAppEventListener() {
    return appEventListener;
  }

  public DependencyResolver getDependencyResolver() {
    return dependencyResolver;
  }

  public InterpreterSettingManager getInterpreterSettingManager() {
    return interpreterSettingManager;
  }

  public InterpreterSetting setAngularObjectRegistryListener(AngularObjectRegistryListener
                                                   angularObjectRegistryListener) {
    this.angularObjectRegistryListener = angularObjectRegistryListener;
    return this;
  }

  public InterpreterSetting setAppEventListener(ApplicationEventListener appEventListener) {
    this.appEventListener = appEventListener;
    return this;
  }

  public InterpreterSetting setRemoteInterpreterProcessListener(RemoteInterpreterProcessListener
                                                      remoteInterpreterProcessListener) {
    this.remoteInterpreterProcessListener = remoteInterpreterProcessListener;
    return this;
  }

  public InterpreterSetting setDependencyResolver(DependencyResolver dependencyResolver) {
    this.dependencyResolver = dependencyResolver;
    return this;
  }

  public InterpreterSetting setInterpreterSettingManager(
      InterpreterSettingManager interpreterSettingManager) {
    this.interpreterSettingManager = interpreterSettingManager;
    return this;
  }

  public InterpreterSetting setLifecycleManager(LifecycleManager lifecycleManager) {
    this.lifecycleManager = lifecycleManager;
    return this;
  }

  public InterpreterSetting setRecoveryStorage(RecoveryStorage recoveryStorage) {
    this.recoveryStorage = recoveryStorage;
    return this;
  }

  public RecoveryStorage getRecoveryStorage() {
    return recoveryStorage;
  }

  public LifecycleManager getLifecycleManager() {
    return lifecycleManager;
  }

  public String getId() {
    return id;
  }

  public String getName() {
    return name;
  }

  public String getGroup() {
    return group;
  }

  /**
   * TODO 用于根据解释器绑定模式生成解释器Group ID（组ID用于进程隔离）
   *    http://zeppelin.apache.org/docs/0.8.1/usage/interpreter/interpreter_binding_mode.html
   * @param user
   * @param noteId
   * @return
   */
  private String getInterpreterGroupId(String user, String noteId) {
    String key;
    if (option.isExistingProcess) {
      // TODO 用于连接预先手动启动的解释器进程
      key = Constants.EXISTING_PROCESS;
    } else if (getOption().isProcess()) {
      // TODO 按用户或者NoteId来设置解释器组（Per-user isolated 或者 Per-note isolated)
      key = (option.perUserIsolated() ? user : "") + ":" + (option.perNoteIsolated() ? noteId : "");
    } else {
      // TODO globally shared 、 per-user scope和per-note scope都是共享一个JVM进程的
      key = SHARED_PROCESS;
    }

    //TODO(zjffdu) we encode interpreter setting id into groupId, this is not a good design
    // TODO 解释器ID + 解释器进程模式的组合作为解释器组ID，用于隔离进程
    return id + ":" + key;
  }

  /**
   * TODO 用于根据解释器绑定模式生成解释器Session ID  SESSION ID用于隔离连接解释器进程的会话
   *  http://zeppelin.apache.org/docs/0.8.1/usage/interpreter/interpreter_binding_mode.html
   * @param user
   * @param noteId
   * @return
   */
  private String getInterpreterSessionId(String user, String noteId) {
    String key;
    if (option.isExistingProcess()) {
      // TODO 用于处理连接已经手动启动的解释器
      key = Constants.EXISTING_PROCESS;
    } else if (option.perNoteScoped() && option.perUserScoped()) {
      // TODO 这个条件永远不会满足，页面上per-note 和per-user是互斥选项
      key = user + ":" + noteId;
    } else if (option.perUserScoped()) {
      // TODO per-user scope时 连接共享解释器进程的sessionId为用户名称，这会出现同一个解释器进程有多个user连接
      key = user;
    } else if (option.perNoteScoped()) {
      // TODO per-note scope时 连接共享解释器进程的sessionId为noteId，这会出现同一个解释器进程有多个Note连接
      key = noteId;
    } else {
      // TODO 对于Globally shared 、per-user isolated以及per-note isolated模式，sessionId都为shared_session
      //   Globally shared: 所有用户的相同类型的段落都共用一个会话去和同一个对应类型的解释器JVM进程进行交互
      //   per-user isolated:一个用户一个解释器对应一个进程，这个用户的所有和解释器类型对应的段落都是通过一个会话去和对应的解释器进程进行交互
      //   per-note isolated: 一个note按note对应的解释器类型独享一个进程，这个note中所有和这个解释器进程类型相同的段落都通过同一个会话器和这个解释器进程交互
      key = SHARED_SESSION;
    }

    return key;
  }

  public ManagedInterpreterGroup getOrCreateInterpreterGroup(String user, String noteId) {
    String groupId = getInterpreterGroupId(user, noteId);
    try {
      interpreterGroupWriteLock.lock();
      if (!interpreterGroups.containsKey(groupId)) {
        LOGGER.info("Create InterpreterGroup with groupId: {} for user: {} and note: {}",
            groupId, user, noteId);
        ManagedInterpreterGroup intpGroup = createInterpreterGroup(groupId);
        interpreterGroups.put(groupId, intpGroup);
      }
      return interpreterGroups.get(groupId);
    } finally {
      interpreterGroupWriteLock.unlock();
    }
  }

  void removeInterpreterGroup(String groupId) {
    try {
      interpreterGroupWriteLock.lock();
      this.interpreterGroups.remove(groupId);
    } finally {
      interpreterGroupWriteLock.unlock();
    }
  }

  public ManagedInterpreterGroup getInterpreterGroup(String user, String noteId) {
    // TODO 获取解释器组ID
    String groupId = getInterpreterGroupId(user, noteId);
    try {
      interpreterGroupReadLock.lock();
      return interpreterGroups.get(groupId);
    } finally {
      interpreterGroupReadLock.unlock();
    }
  }

  ManagedInterpreterGroup getInterpreterGroup(String groupId) {
    return interpreterGroups.get(groupId);
  }

  public ArrayList<ManagedInterpreterGroup> getAllInterpreterGroups() {
    try {
      interpreterGroupReadLock.lock();
      return new ArrayList(interpreterGroups.values());
    } finally {
      interpreterGroupReadLock.unlock();
    }
  }

  Map<String, Object> getEditorFromSettingByClassName(String className) {
    for (InterpreterInfo intpInfo : interpreterInfos) {
      if (className.equals(intpInfo.getClassName())) {
        if (intpInfo.getEditor() == null) {
          break;
        }
        return intpInfo.getEditor();
      }
    }
    return DEFAULT_EDITOR;
  }

  /**
   * TODO 处理notebook页面的重启解释器请求
   * @param user
   * @param noteId
   */
  void closeInterpreters(String user, String noteId) {
    // TODO 到这里的InterpreterSetting已经是和用户要重启的类型一致了
    //  获取解释器组
    ManagedInterpreterGroup interpreterGroup = getInterpreterGroup(user, noteId);
    if (interpreterGroup != null) {
      // TODO 获取用户ID以及noteId对应的哪个解释器sessionId（这里的session不是用户和ZeppelinServer之间的Session，而是
      //  ZeppelinServer和各个解释器实例之间维护的Session，所以一个用户可能会对应很多个Session，需要加上InterpreterSetting和
      //  NoteId来唯一确定用户的对应类型的解释器和ZeppelinServer之间的Session）
      String sessionId = getInterpreterSessionId(user, noteId);
      interpreterGroup.close(sessionId);
    }
  }

  public void close() {
    LOGGER.info("Close InterpreterSetting: " + name);
    for (ManagedInterpreterGroup intpGroup : interpreterGroups.values()) {
      intpGroup.close();
    }
    interpreterGroups.clear();
    this.runtimeInfosToBeCleared = null;
    this.infos = null;
  }

  public void setProperties(Object object) {
    if (object instanceof StringMap) {
      StringMap<String> map = (StringMap) properties;
      Properties newProperties = new Properties();
      for (String key : map.keySet()) {
        newProperties.put(key, map.get(key));
      }
      this.properties = newProperties;
    } else {
      this.properties = object;
    }
  }


  public Object getProperties() {
    return properties;
  }

  @VisibleForTesting
  public void setProperty(String name, String value) {
    ((Map<String, InterpreterProperty>) properties).put(name, new InterpreterProperty(name, value));
  }

  // This method is supposed to be only called by InterpreterSetting
  // but not InterpreterSetting Template
  public Properties getJavaProperties() {
    Properties jProperties = new Properties();
    Map<String, InterpreterProperty> iProperties = (Map<String, InterpreterProperty>) properties;
    for (Map.Entry<String, InterpreterProperty> entry : iProperties.entrySet()) {
      if (entry.getValue().getValue() != null) {
        jProperties.setProperty(entry.getKey(), entry.getValue().getValue().toString());
      }
    }

    if (!jProperties.containsKey("zeppelin.interpreter.output.limit")) {
      jProperties.setProperty("zeppelin.interpreter.output.limit",
          conf.getInt(ZEPPELIN_INTERPRETER_OUTPUT_LIMIT) + "");
    }

    if (!jProperties.containsKey("zeppelin.interpreter.max.poolsize")) {
      jProperties.setProperty("zeppelin.interpreter.max.poolsize",
          conf.getInt(ZEPPELIN_INTERPRETER_MAX_POOL_SIZE) + "");
    }

    String interpreterLocalRepoPath = conf.getInterpreterLocalRepoPath();
    //TODO(zjffdu) change it to interpreterDir/{interpreter_name}
    jProperties.setProperty("zeppelin.interpreter.localRepo",
        interpreterLocalRepoPath + "/" + id);
    return jProperties;
  }

  public ZeppelinConfiguration getConf() {
    return conf;
  }

  public InterpreterSetting setConf(ZeppelinConfiguration conf) {
    this.conf = conf;
    return this;
  }

  public List<Dependency> getDependencies() {
    return dependencies;
  }

  public void setDependencies(List<Dependency> dependencies) {
    this.dependencies = dependencies;
    loadInterpreterDependencies();
  }

  public InterpreterOption getOption() {
    return option;
  }

  public void setOption(InterpreterOption option) {
    this.option = option;
  }

  public String getInterpreterDir() {
    return interpreterDir;
  }

  public void setInterpreterDir(String interpreterDir) {
    this.interpreterDir = interpreterDir;
  }

  public List<InterpreterInfo> getInterpreterInfos() {
    return interpreterInfos;
  }

  void appendDependencies(List<Dependency> dependencies) {
    for (Dependency dependency : dependencies) {
      if (!this.dependencies.contains(dependency)) {
        this.dependencies.add(dependency);
      }
    }
    loadInterpreterDependencies();
  }

  void setInterpreterOption(InterpreterOption interpreterOption) {
    this.option = interpreterOption;
  }

  public void setProperties(Properties p) {
    this.properties = p;
  }

  void setGroup(String group) {
    this.group = group;
  }

  void setName(String name) {
    this.name = name;
  }

  /***
   * Interpreter status
   */
  public enum Status {
    DOWNLOADING_DEPENDENCIES,
    ERROR,
    READY
  }

  public Status getStatus() {
    return status;
  }

  public void setStatus(Status status) {
    this.status = status;
  }

  public String getErrorReason() {
    return errorReason;
  }

  public void setErrorReason(String errorReason) {
    this.errorReason = errorReason;
  }

  public void setInterpreterInfos(List<InterpreterInfo> interpreterInfos) {
    this.interpreterInfos = interpreterInfos;
  }

  public void setInfos(Map<String, String> infos) {
    this.infos = infos;
  }

  public Map<String, String> getInfos() {
    return infos;
  }

  public InterpreterRunner getInterpreterRunner() {
    return interpreterRunner;
  }

  public void setInterpreterRunner(InterpreterRunner interpreterRunner) {
    this.interpreterRunner = interpreterRunner;
  }

  public void addNoteToPara(String noteId, String paraId) {
    if (runtimeInfosToBeCleared == null) {
      runtimeInfosToBeCleared = new HashMap<>();
    }
    Set<String> paraIdSet = runtimeInfosToBeCleared.get(noteId);
    if (paraIdSet == null) {
      paraIdSet = new HashSet<>();
      runtimeInfosToBeCleared.put(noteId, paraIdSet);
    }
    paraIdSet.add(paraId);
  }

  public Map<String, Set<String>> getNoteIdAndParaMap() {
    return runtimeInfosToBeCleared;
  }

  public void clearNoteIdAndParaMap() {
    runtimeInfosToBeCleared = null;
  }


  //////////////////////////// IMPORTANT ////////////////////////////////////////////////
  ///////////////////////////////////////////////////////////////////////////////////////
  ///////////////////////////////////////////////////////////////////////////////////////
  // This is the only place to create interpreters. For now we always create multiple interpreter
  // together (one session). We don't support to create single interpreter yet.
  List<Interpreter> createInterpreters(String user, String interpreterGroupId, String sessionId) {
    List<Interpreter> interpreters = new ArrayList<>();
    List<InterpreterInfo> interpreterInfos = getInterpreterInfos();
    Properties intpProperties = getJavaProperties();
    for (InterpreterInfo info : interpreterInfos) {
      // TODO 创建解释器客户端
      Interpreter interpreter = new RemoteInterpreter(intpProperties, sessionId,
          info.getClassName(), user, lifecycleManager);
      if (info.isDefaultInterpreter()) {
        interpreters.add(0, interpreter);
      } else {
        interpreters.add(interpreter);
      }
      LOGGER.info("Interpreter {} created for user: {}, sessionId: {}",
          interpreter.getClassName(), user, sessionId);
    }

    // TODO(zjffdu) this kind of hardcode is ugly. For now SessionConfInterpreter is used
    // for livy, we could add new property in interpreter-setting.json when there's new interpreter
    // require SessionConfInterpreter
    if (group.equals("livy")) {
      interpreters.add(
          new SessionConfInterpreter(intpProperties, sessionId, interpreterGroupId, this));
    } else {
      interpreters.add(new ConfInterpreter(intpProperties, sessionId, interpreterGroupId, this));
    }
    return interpreters;
  }

  synchronized RemoteInterpreterProcess createInterpreterProcess(String interpreterGroupId,
                                                                 String userName,
                                                                 Properties properties)
      throws IOException {
    if (launcher == null) {
      createLauncher();
    }
    InterpreterLaunchContext launchContext = new
        InterpreterLaunchContext(properties, option, interpreterRunner, userName,
        interpreterGroupId, id, group, name);
    // TODO 解释器启动器构建入口
    RemoteInterpreterProcess process = (RemoteInterpreterProcess) launcher.launch(launchContext);
    process.setRemoteInterpreterEventPoller(
        new RemoteInterpreterEventPoller(remoteInterpreterProcessListener, appEventListener));
    recoveryStorage.onInterpreterClientStart(process);
    return process;
  }

  /**
   * TODO 组ID隔离解释器进程
   *  session隔离连接组ID对应的解释器进程的会话
   *  所以除了per-user scope 、per-note scope模式是一个解释器进程对应多个session外
   *  globally shared、per-user isolated和per-note isolated模式都是一个解释器进程一个Session,
   *  globally shared模式是本身语义就是如此
   *  per-user isolated和per-note isolated模式应该是不需要再拆分session了，否则资源占用情况会很严重
   *  其实per-user isolated还可以拆成解释器进程对应的用户的每个对应类型的note或者段落一个session，
   *  per-note isolated还可以拆成每个段落一个session，但是这样太细了的话，同一个note中多饿段落的数据会无法共享
   * @param user
   * @param noteId
   * @return
   */
  List<Interpreter> getOrCreateSession(String user, String noteId) {
    // TODO 获取解释器组ID（会根据解释器绑定模式生成组ID) 组ID用于解释器进程隔离
    ManagedInterpreterGroup interpreterGroup = getOrCreateInterpreterGroup(user, noteId);
    Preconditions.checkNotNull(interpreterGroup, "No InterpreterGroup existed for user {}, " +
        "noteId {}", user, noteId);
    // TODO 根据解释器绑定模式生成会话ID
    String sessionId = getInterpreterSessionId(user, noteId);
    // TODO 有了解释器组ID和SessionId，下面就该获取或者创建对应的解释器进程了（所以这里是创建解释器进程的入口）
    return interpreterGroup.getOrCreateSession(user, sessionId);
  }

  public Interpreter getDefaultInterpreter(String user, String noteId) {
    return getOrCreateSession(user, noteId).get(0);
  }

  public Interpreter getInterpreter(String user, String noteId, String replName) {
    Preconditions.checkNotNull(noteId, "noteId should be not null");
    Preconditions.checkNotNull(replName, "replName should be not null");

    String className = getInterpreterClassFromInterpreterSetting(replName);
    if (className == null) {
      return null;
    }
    List<Interpreter> interpreters = getOrCreateSession(user, noteId);
    for (Interpreter interpreter : interpreters) {
      if (className.equals(interpreter.getClassName())) {
        return interpreter;
      }
    }
    return null;
  }

  private String getInterpreterClassFromInterpreterSetting(String replName) {
    Preconditions.checkNotNull(replName, "replName should be not null");

    for (InterpreterInfo info : interpreterInfos) {
      String infoName = info.getName();
      if (null != info.getName() && replName.equals(infoName)) {
        return info.getClassName();
      }
    }
    //TODO(zjffdu) It requires user can not create interpreter with name `conf`,
    // conf is a reserved word of interpreter name
    if (replName.equals("conf")) {
      if (group.equals("livy")) {
        return SessionConfInterpreter.class.getName();
      } else {
        return ConfInterpreter.class.getName();
      }
    }
    return null;
  }

  private ManagedInterpreterGroup createInterpreterGroup(String groupId) {
    AngularObjectRegistry angularObjectRegistry;
    ManagedInterpreterGroup interpreterGroup = new ManagedInterpreterGroup(groupId, this);
    angularObjectRegistry =
        new RemoteAngularObjectRegistry(groupId, angularObjectRegistryListener, interpreterGroup);
    interpreterGroup.setAngularObjectRegistry(angularObjectRegistry);
    return interpreterGroup;
  }

  /**
   * Throw exception when interpreter process has already launched
   *
   * @param interpreterGroupId
   * @param properties
   * @throws IOException
   */
  public void setInterpreterGroupProperties(String interpreterGroupId, Properties properties)
      throws IOException {
    ManagedInterpreterGroup interpreterGroup = this.interpreterGroups.get(interpreterGroupId);
    for (List<Interpreter> session : interpreterGroup.sessions.values()) {
      for (Interpreter intp : session) {
        if (!intp.getProperties().equals(properties) &&
            interpreterGroup.getRemoteInterpreterProcess() != null &&
            interpreterGroup.getRemoteInterpreterProcess().isRunning()) {
          throw new IOException("Can not change interpreter properties when interpreter process " +
              "has already been launched");
        }
        intp.setProperties(properties);
      }
    }
  }

  private void loadInterpreterDependencies() {
    setStatus(Status.DOWNLOADING_DEPENDENCIES);
    setErrorReason(null);
    Thread t = new Thread() {
      public void run() {
        try {
          // dependencies to prevent library conflict
          File localRepoDir = new File(conf.getInterpreterLocalRepoPath() + "/" + id);
          if (localRepoDir.exists()) {
            try {
              FileUtils.forceDelete(localRepoDir);
            } catch (FileNotFoundException e) {
              LOGGER.info("A file that does not exist cannot be deleted, nothing to worry", e);
            }
          }

          // load dependencies
          List<Dependency> deps = getDependencies();
          if (deps != null) {
            for (Dependency d : deps) {
              File destDir = new File(
                  conf.getRelativeDir(ZeppelinConfiguration.ConfVars.ZEPPELIN_DEP_LOCALREPO));

              if (d.getExclusions() != null) {
                dependencyResolver.load(d.getGroupArtifactVersion(), d.getExclusions(),
                    new File(destDir, id));
              } else {
                dependencyResolver
                    .load(d.getGroupArtifactVersion(), new File(destDir, id));
              }
            }
          }

          setStatus(Status.READY);
          setErrorReason(null);
        } catch (Exception e) {
          LOGGER.error(String.format("Error while downloading repos for interpreter group : %s," +
                  " go to interpreter setting page click on edit and save it again to make " +
                  "this interpreter work properly. : %s",
              getGroup(), e.getLocalizedMessage()), e);
          setErrorReason(e.getLocalizedMessage());
          setStatus(Status.ERROR);
        }
      }
    };

    t.start();
  }

  //TODO(zjffdu) ugly code, should not use JsonObject as parameter. not readable
  public void convertPermissionsFromUsersToOwners(JsonObject jsonObject) {
    if (jsonObject != null) {
      JsonObject option = jsonObject.getAsJsonObject("option");
      if (option != null) {
        JsonArray users = option.getAsJsonArray("users");
        if (users != null) {
          if (this.option.getOwners() == null) {
            this.option.owners = new LinkedList<>();
          }
          for (JsonElement user : users) {
            this.option.getOwners().add(user.getAsString());
          }
        }
      }
    }
  }

  // For backward compatibility of interpreter.json format after ZEPPELIN-2403
  static Map<String, InterpreterProperty> convertInterpreterProperties(Object properties) {
    if (properties != null && properties instanceof StringMap) {
      Map<String, InterpreterProperty> newProperties = new HashMap<>();
      StringMap p = (StringMap) properties;
      for (Object o : p.entrySet()) {
        Map.Entry entry = (Map.Entry) o;
        if (!(entry.getValue() instanceof StringMap)) {
          InterpreterProperty newProperty = new InterpreterProperty(
              entry.getKey().toString(),
              entry.getValue(),
              InterpreterPropertyType.STRING.getValue());
          newProperties.put(entry.getKey().toString(), newProperty);
        } else {
          // already converted
          return (Map<String, InterpreterProperty>) properties;
        }
      }
      return newProperties;

    } else if (properties instanceof Map) {
      Map<String, Object> dProperties =
          (Map<String, Object>) properties;
      Map<String, InterpreterProperty> newProperties = new HashMap<>();
      for (String key : dProperties.keySet()) {
        Object value = dProperties.get(key);
        if (value instanceof InterpreterProperty) {
          return (Map<String, InterpreterProperty>) properties;
        } else if (value instanceof StringMap) {
          StringMap stringMap = (StringMap) value;
          InterpreterProperty newProperty = new InterpreterProperty(
              key,
              stringMap.get("value"),
              stringMap.containsKey("type") ? stringMap.get("type").toString() : "string");

          newProperties.put(newProperty.getName(), newProperty);
        } else if (value instanceof DefaultInterpreterProperty){
          DefaultInterpreterProperty dProperty = (DefaultInterpreterProperty) value;
          InterpreterProperty property = new InterpreterProperty(
              key,
              dProperty.getValue(),
              dProperty.getType() != null ? dProperty.getType() : "string"
              // in case user forget to specify type in interpreter-setting.json
          );
          newProperties.put(key, property);
        } else if (value instanceof String) {
          InterpreterProperty newProperty = new InterpreterProperty(
              key,
              value,
              "string");

          newProperties.put(newProperty.getName(), newProperty);
        } else {
          throw new RuntimeException("Can not convert this type of property: " +
              value.getClass());
        }
      }
      return newProperties;
    }
    throw new RuntimeException("Can not convert this type: " + properties.getClass());
  }

  public void waitForReady() throws InterruptedException {
    while (getStatus().equals(
        org.apache.zeppelin.interpreter.InterpreterSetting.Status.DOWNLOADING_DEPENDENCIES)) {
      Thread.sleep(200);
    }
  }
}
