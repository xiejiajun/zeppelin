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

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringReader;
import java.util.Properties;

/**
 * Special Interpreter for Interpreter Configuration customization. It is attached to each
 * InterpreterGroup implicitly by Zeppelin.
 */
public class ConfInterpreter extends Interpreter {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConfInterpreter.class);

  protected String sessionId;
  protected String interpreterGroupId;
  protected InterpreterSetting interpreterSetting;


  public ConfInterpreter(Properties properties,
                         String sessionId,
                         String interpreterGroupId,
                         InterpreterSetting interpreterSetting) {
    super(properties);
    this.sessionId = sessionId;
    this.interpreterGroupId = interpreterGroupId;
    this.interpreterSetting = interpreterSetting;
  }

  @Override
  public void open() throws InterpreterException {

  }

  @Override
  public void close() throws InterpreterException {

  }

  @Override
  public InterpreterResult interpret(String st, InterpreterContext context)
      throws InterpreterException {

    try {
      Properties finalProperties = new Properties();
      finalProperties.putAll(getProperties());
      Properties newProperties = new Properties();
      // TODO(Luffy) 解析并读取以=、:、空格、\t、\f等分隔的K V配置
      newProperties.load(new StringReader(st));
      finalProperties.putAll(newProperties);
      LOGGER.debug("Properties for InterpreterGroup: {} is {}", interpreterGroupId, finalProperties);
      // TODO(Luffy) 将新配置更新到interpreterGroupId(决定了对应的解释器进程是哪一个)对应的interpreterSetting中
      //  分析InterpreterSetting.setInterpreterGroupProperties源码得知：这里更新的是已经创建但还未open的相应解释器的
      //  RemoteInterpreter实例的properties字段，不是修改interpreterSetting.properties字段，因为那样修改的是整个解释器
      //  组的全局配置，会影响到所有使用该解释器类型的解释器实例，肯定是不对的，这里的做法是一种巧妙的用户自定义配置隔离
      interpreterSetting.setInterpreterGroupProperties(interpreterGroupId, finalProperties);
      return new InterpreterResult(InterpreterResult.Code.SUCCESS);
    } catch (IOException e) {
      LOGGER.error("Fail to update interpreter setting", e);
      return new InterpreterResult(InterpreterResult.Code.ERROR, ExceptionUtils.getStackTrace(e));
    }
  }

  @Override
  public void cancel(InterpreterContext context) throws InterpreterException {

  }

  @Override
  public FormType getFormType() throws InterpreterException {
    return FormType.NATIVE;
  }

  @Override
  public int getProgress(InterpreterContext context) throws InterpreterException {
    return 0;
  }
}
