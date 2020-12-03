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

import com.google.gson.Gson;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.thrift.TException;
import org.apache.zeppelin.interpreter.launcher.InterpreterClient;
import org.apache.zeppelin.interpreter.thrift.RemoteInterpreterService.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static org.apache.zeppelin.conf.InterpreterConfiguration.*;

/**
 * Abstract class for interpreter process
 */
public abstract class RemoteInterpreterProcess implements InterpreterClient {
  private static final Logger logger = LoggerFactory.getLogger(RemoteInterpreterProcess.class);

  private GenericObjectPool<Client> clientPool;
  private RemoteInterpreterEventPoller remoteInterpreterEventPoller;
  private final InterpreterContextRunnerPool interpreterContextRunnerPool;
  private int connectTimeout;
  private ClientFactory clientFactory = null;
  protected Map<String, String> env;

  public RemoteInterpreterProcess(
      int connectTimeout,Map<String, String> env) {
    this.interpreterContextRunnerPool = new InterpreterContextRunnerPool();
    this.connectTimeout = connectTimeout;
    this.env = env;
    // TODO 不能在构造方法里面初始化，因为这个时候解释器的端口和host都还为null
//    this.createClientPool();
  }


  /**
   * create thrift client pool
   */
  private synchronized void createClientPool(){
    int maxIdle = DEFAULT_ZEPPELIN_THRIFT_CLIENT_POOL_MAX_IDLE;
    int minIdle = DEFAULT_ZEPPELIN_THRIFT_CLIENT_POOL_MIN_IDLE;
    int maxTotal = DEFAULT_ZEPPELIN_THRIFT_CLIENT_POOL_MAX_TOTAL;
    if (MapUtils.isNotEmpty(this.env)) {
      maxTotal = formatInteger(env.get(ZEPPELIN_THRIFT_CLIENT_POOL_MAX_TOTAL),
              DEFAULT_ZEPPELIN_THRIFT_CLIENT_POOL_MAX_TOTAL);
      minIdle = formatInteger(env.get(ZEPPELIN_THRIFT_CLIENT_POOL_MIN_IDLE),
              DEFAULT_ZEPPELIN_THRIFT_CLIENT_POOL_MIN_IDLE);
      maxIdle = formatInteger(env.get(ZEPPELIN_THRIFT_CLIENT_POOL_MAX_IDLE),
              DEFAULT_ZEPPELIN_THRIFT_CLIENT_POOL_MAX_IDLE);
    }
    if (clientPool == null || clientPool.isClosed()) {
      clientFactory = new ClientFactory(getHost(), getPort());
      clientPool = new GenericObjectPool<>(clientFactory);
      clientPool.setMaxIdle(maxIdle);
      clientPool.setMinIdle(minIdle);
      clientPool.setMaxTotal(maxTotal);
      // TODO 这里加上获取Thrift客户端2秒超时，否则默认-1永不超时，手动kill -9强制杀掉解释器进程后可能会导致无法重启对应解释器（一直阻塞转圈）
      clientPool.setMaxWaitMillis(2000);
    }
  }


  public RemoteInterpreterEventPoller getRemoteInterpreterEventPoller() {
    return remoteInterpreterEventPoller;
  }

  public void setRemoteInterpreterEventPoller(RemoteInterpreterEventPoller eventPoller) {
    this.remoteInterpreterEventPoller = eventPoller;
  }

  public void shutdown() {

    // Close client socket connection
    if (clientFactory != null) {
      clientFactory.close();
    }
  }

  public int getConnectTimeout() {
    return connectTimeout;
  }

  // TODO 这种写法如果某个解释器的option为globally shared，同时又有大量用户在用这个解释器的话，会造成大量线程BLOCKED在这里，从而导致服务卡顿
//  public synchronized Client getClient() throws Exception {
//    if (clientPool == null || clientPool.isClosed()) {
//      clientPool = new GenericObjectPool<>(new ClientFactory(getHost(), getPort()));
//    }
//    return clientPool.borrowObject();
//  }


  public Client getClient() throws Exception {
    if (clientPool == null || clientPool.isClosed()){
      createClientPool();
    }
    return clientPool.borrowObject();
  }

  private void releaseClient(Client client) {
    releaseClient(client, false);
  }

  private void releaseClient(Client client, boolean broken) {
    if (broken) {
      releaseBrokenClient(client);
    } else {
      try {
        clientPool.returnObject(client);
      } catch (Exception e) {
        logger.warn("exception occurred during releasing thrift client", e);
      }
    }
  }

  private void releaseBrokenClient(Client client) {
    try {
      clientPool.invalidateObject(client);
    } catch (Exception e) {
      logger.warn("exception occurred during releasing thrift client", e);
    }
  }

  /**
   * Called when angular object is updated in client side to propagate
   * change to the remote process
   * @param name
   * @param o
   */
  public void updateRemoteAngularObject(String name, String noteId, String paragraphId, Object o) {
    Client client = null;
    try {
      client = getClient();
    } catch (NullPointerException e) {
      // remote process not started
      logger.info("NullPointerException in RemoteInterpreterProcess while " +
          "updateRemoteAngularObject getClient, remote process not started", e);
      return;
    } catch (Exception e) {
      logger.error("Can't update angular object", e);
    }

    boolean broken = false;
    try {
      Gson gson = new Gson();
      client.angularObjectUpdate(name, noteId, paragraphId, gson.toJson(o));
    } catch (TException e) {
      broken = true;
      logger.error("Can't update angular object", e);
    } catch (NullPointerException e) {
      logger.error("Remote interpreter process not started", e);
      return;
    } finally {
      if (client != null) {
        releaseClient(client, broken);
      }
    }
  }

  public InterpreterContextRunnerPool getInterpreterContextRunnerPool() {
    return interpreterContextRunnerPool;
  }

  public <T> T callRemoteFunction(RemoteFunction<T> func) {
    Client client = null;
    boolean broken = false;
    try {
      client = getClient();
      if (client != null) {
        return func.call(client);
      }
    } catch (TException e) {
      broken = true;
      throw new RuntimeException(e);
    } catch (Exception e1) {
      throw new RuntimeException(e1);
    } finally {
      if (client != null) {
        releaseClient(client, broken);
      }
    }
    return null;
  }

  /**
   * OpenJDK int不能自动转为Integer
   * String to Integer
   * @param intString
   * @param defaultValue
   * @return
   */
  private int formatInteger(String intString, int defaultValue){
    try {
      return NumberUtils.createInteger(intString);
    } catch (Exception e){
      return defaultValue;
    }
  }

  /**
   *
   * @param <T>
   */
  public interface RemoteFunction<T> {
    T call(Client client) throws Exception;
  }
}
