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

package org.apache.zeppelin.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.slf4j.Logger;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * java.util.Properties read utils
 */
public class PropertiesUtil {

  /**
   * read the string property
   * @param properties the properties object
   * @param key the property key
   * @param defaultValue the default value
   * @return the string result
   */
  public static String getString(Properties properties, String key, String defaultValue) {
    return properties.getProperty(key) == null ? defaultValue : properties.getProperty(key);
  }


  /**
   * read the long property
   * @param properties the properties object
   * @param key the property key
   * @param defaultValue the default value
   * @return the long result
   */
  public static long getLong(Properties properties, String key, long defaultValue) {
    String valueString = properties.getProperty(key);
    if (valueString == null){
      return defaultValue;
    }
    try {
      return Long.parseLong(valueString);
    }catch (Exception e){
      return defaultValue;
    }
  }

  /**
   * read the long property
   * @param properties the properties object
   * @param key the property key
   * @param defaultValue the default value
   * @return the int result
   */
  public static int getInt(Properties properties, String key, int defaultValue) {
    String valueString = properties.getProperty(key);
    if (valueString == null){
      return defaultValue;
    }
    try {
      return Integer.parseInt(valueString);
    }catch (Exception e){
      return defaultValue;
    }
  }


  /**
   * Replace markers #{contextFieldName} by values from {@link InterpreterContext} fields
   * with same name and marker #{user}. If value == null then replace by empty string.
   */
  public static void replaceContextParameters(Properties properties, Logger LOGGER, String userName) {
    InterpreterContext interpreterContext = InterpreterContext.get();
    if (interpreterContext != null) {
      String markerTemplate = "#\\{%s\\}";
      List<String> skipFields = Arrays.asList("paragraphTitle", "paragraphId", "paragraphText");
      List<Class<?>> typesToProcess = Arrays.asList(String.class, Double.class, Float.class, Short.class,
              Byte.class, Character.class, Boolean.class, Integer.class, Long.class);
      for (String key : properties.stringPropertyNames()) {
        String p = properties.getProperty(key);
        if (StringUtils.isNotEmpty(p)) {
          for (Field field : InterpreterContext.class.getDeclaredFields()) {
            Class<?> clazz = field.getType();
            if (!skipFields.contains(field.getName()) && (typesToProcess.contains(clazz)
                    || clazz.isPrimitive())) {
              Object value = null;
              try {
                value = FieldUtils.readField(field, interpreterContext, true);
              } catch (Exception e) {
                LOGGER.error("Cannot read value of field {}", field.getName());
              }
              p = p.replaceAll(String.format(markerTemplate, field.getName()),
                      value != null ? value.toString() : StringUtils.EMPTY);
            }
          }
          p = p.replaceAll(String.format(markerTemplate, "user"),
                  StringUtils.defaultString(userName, StringUtils.EMPTY));
          properties.setProperty(key, p);
        }
      }
    }
  }
}
