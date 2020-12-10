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

import javax.inject.Inject;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * //TODO(zjffdu) considering to move to InterpreterSettingManager
 *
 * Factory class for creating interpreters.
 *
 */
public class InterpreterFactory implements InterpreterFactoryInterface {
  private static final Logger LOGGER = LoggerFactory.getLogger(InterpreterFactory.class);

  private final InterpreterSettingManager interpreterSettingManager;

  @Inject
  public InterpreterFactory(InterpreterSettingManager interpreterSettingManager) {
    this.interpreterSettingManager = interpreterSettingManager;
  }

  @Override
  public Interpreter getInterpreter(String replName,
                                    ExecutionContext executionContext)
      throws InterpreterNotFoundException {

    if (StringUtils.isBlank(replName)) {
      // Get the default interpreter of the defaultInterpreterSetting
      InterpreterSetting defaultSetting =
          interpreterSettingManager.getByName(executionContext.getDefaultInterpreterGroup());
      return defaultSetting.getDefaultInterpreter(executionContext);
    }

    String[] replNameSplits = replName.split("\\.");
    if (replNameSplits.length == 2) {
      // TODO(Luffy) 如spark.sql saprk.r、spark.pyspark、spark.conf、hive.conf等等
      String group = replNameSplits[0];
      String name = replNameSplits[1];
      // TODO(Luffy) 根据解释器name获取对应的解释器配置对象InterpreterSetting,
      //  解释器相关的即诶生气配置页面上配置的全局配置都在这里
      InterpreterSetting setting = interpreterSettingManager.getByName(group);
      if (null != setting) {
        // TODO(Luffy) ConfInterpreter的获取也是在这里完成的
        Interpreter interpreter = setting.getInterpreter(executionContext, name);
        if (null != interpreter) {
          return interpreter;
        }
        throw new InterpreterNotFoundException("No such interpreter: " + replName);
      }
      throw new InterpreterNotFoundException("No interpreter setting named: " + group);

    } else if (replNameSplits.length == 1){
      // first assume group is omitted
      // TODO(Luffy) 根据note默认解释器获取setting: 对应note默认解释器为spark,内部通过pyspark、sql等指定解释器的情况
      //  Note默认解释器为spark时通过%conf配置个性化配置也是在这里处理，注意：若Note的默认解释器是hive这种只有一级的，除支持
      //  %hive.conf，一样也支持%conf进行个性化配置
      InterpreterSetting setting =
          interpreterSettingManager.getByName(executionContext.getDefaultInterpreterGroup());
      if (setting != null) {
        Interpreter interpreter = setting.getInterpreter(executionContext, replName);
        if (null != interpreter) {
          return interpreter;
        }
      }

      // then assume interpreter name is omitted
      // TODO(Luffy) 对应解释器名称只要一级且Note的默认解释器不是该解释器类型的情况: 如在hive、sh、presto等在默认解释器不是它们
      //  自己的Note中使用时
      setting = interpreterSettingManager.getByName(replName);
      if (null != setting) {
        return setting.getDefaultInterpreter(executionContext);
      }
    }

    throw new InterpreterNotFoundException("No such interpreter: " + replName);
  }
}
