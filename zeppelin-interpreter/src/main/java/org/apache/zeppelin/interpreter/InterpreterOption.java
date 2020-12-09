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

import java.util.ArrayList;
import java.util.List;

import com.google.gson.Gson;
import org.apache.zeppelin.conf.ZeppelinConfiguration;

/**
 *
 */
public class InterpreterOption {
  public static final transient String SHARED = "shared";
  public static final transient String SCOPED = "scoped";
  public static final transient String ISOLATED = "isolated";
  private static ZeppelinConfiguration conf =  ZeppelinConfiguration.create();

  // always set it as true, keep this field just for backward compatibility
  boolean remote = true;
  String host = null;
  int port = -1;

  String perNote;
  String perUser;

  boolean isExistingProcess;
  boolean setPermission;
  List<String> owners;
  boolean isUserImpersonate;

  public boolean isExistingProcess() {
    return isExistingProcess;
  }

  public void setExistingProcess(boolean isExistingProcess) {
    this.isExistingProcess = isExistingProcess;
  }

  public void setPort(int port) {
    this.port = port;
  }

  public void setHost(String host) {
    this.host = host;
  }

  public boolean permissionIsSet() {
    return setPermission;
  }

  public void setUserPermission(boolean setPermission) {
    this.setPermission = setPermission;
  }

  public List<String> getOwners() {
    if (null != owners && conf.isUsernameForceLowerCase()) {
      List<String> lowerCaseUsers = new ArrayList<String>();
      for (String owner : owners) {
        lowerCaseUsers.add(owner.toLowerCase());
      }
      return lowerCaseUsers;
    }
    return owners;
  }

  public boolean isUserImpersonate() {
    return isUserImpersonate;
  }

  public void setUserImpersonate(boolean userImpersonate) {
    isUserImpersonate = userImpersonate;
  }

  public InterpreterOption() {
  }

  public InterpreterOption(String perUser, String perNote) {
    if (perUser == null) {
      throw new NullPointerException("perUser can not be null.");
    }
    if (perNote == null) {
      throw new NullPointerException("perNote can not be null.");
    }

    this.perUser = perUser;
    this.perNote = perNote;
  }

  public static InterpreterOption fromInterpreterOption(InterpreterOption other) {
    InterpreterOption option = new InterpreterOption();
    option.remote = other.remote;
    option.host = other.host;
    option.port = other.port;
    option.perNote = other.perNote;
    option.perUser = other.perUser;
    option.isExistingProcess = other.isExistingProcess;
    option.setPermission = other.setPermission;
    option.owners = (null == other.owners) ?
        new ArrayList<String>() : new ArrayList<>(other.owners);

    return option;
  }

  public String getHost() {
    return host;
  }

  public int getPort() {
    return port;
  }


  /**
   * 同一类型的Paragraph不分用户、也不分Note，都共享同一个解释器进程、一个Session
   * @return
   */
  public boolean perUserShared() {
    return SHARED.equals(perUser);
  }

  /**
   * 整个Notebook中相同解释器类型的所有Paragraph共用一个解释器进程，但是每个用户运行时都有自己专用的session(
   *  即同一用户的所有相同类型的Paragraph共用一个session，哪怕它们不在同一个Note中)
   * 这种模式下仍然可以使用基于ZeppelinContext的ResourcePool相互交换数据:z.put、z.get、z.show
   * http://zeppelin.apache.org/docs/0.8.2/usage/other_features/zeppelin_context.html
   * @return
   */
  public boolean perUserScoped() {
    return SCOPED.equals(perUser);
  }

  /**
   * 每个用户的相同解释器类型的所有Paragraph共用一个解释器进程，哪怕它们在不同的Note中，(隔离模式都是一个解释器进程只有一个session）,
   * 这种模式下仍然可以使用基于ZeppelinContext的ResourcePool相互交换数据:z.put、z.get、z.show
   * 这种情况使用的是分布式资源池DistributedResourcePool
   * @return
   */
  public boolean perUserIsolated() {
    return ISOLATED.equals(perUser);
  }

  /**
   * 同一个Note下面的相同解释器类型的所有用户的所有Paragraph共享一个解释器进程、一个Session
   * @return
   */
  public boolean perNoteShared() {
    return SHARED.equals(perNote);
  }

  /**
   * 整个Notebook中相同解释器类型的所有Paragraph共用一个解释器进程，但每个Note运行时都有自己专用的session，即使是属于同一用户启动的，
   *  也会有自己独立的session，这就是和perUserScoped模式的区别, perUserScoped模式每个用户的所有相同类型的paragraph都
   *  共用一个Session，而perNoteScoped是每个Note下相同类型的Paragraph共用一个Session，同一用户有多个Note都有
   *  对应类型的paragraph的话这个用户就会有多个Session
   * 这种模式下仍然可以使用基于ZeppelinContext的ResourcePool相互交换数据:z.put、z.get、z.show
   * @return
   */
  public boolean perNoteScoped() {
    return SCOPED.equals(perNote);
  }

  /**
   * 同一个Note中的所有类型相同的paragraph共用一个解释器进程(隔离模式都是一个解释器进程只有一个session), 就算是同一用户启动的相同类型的paragraph
   *  只要不在同一个Note里面就会使用不同的解释器进程，这是和perUserIsolated模式的区别，它比perUserIsolated模式隔离粒度更细
   * 这种情况可以分布式资源池DistributedResourcePool进行数据交换: :z.put、z.get、z.show
   * @return
   */
  public boolean perNoteIsolated() {
    return ISOLATED.equals(perNote);
  }

  /**
   * 用于判断是否是隔离模式：perNoteIsolated或perUserIsolated
   * @return
   */
  public boolean isIsolated() {
    return perUserIsolated() || perNoteIsolated();
  }

  /**
   * @return
   */
  public boolean isSession() {
    return perUserScoped() || perNoteScoped();
  }

  public void setPerNote(String perNote) {
    this.perNote = perNote;
  }

  public void setPerUser(String perUser) {
    this.perUser = perUser;
  }
}
