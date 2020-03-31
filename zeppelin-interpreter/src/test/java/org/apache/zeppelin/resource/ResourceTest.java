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
package org.apache.zeppelin.resource;

import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;

/**
 * Test for Resource
 */
public class ResourceTest {
  @Test
  public void testSerializeDeserialize() throws IOException, ClassNotFoundException {
    ByteBuffer buffer = Resource.serializeObject("hello");
    assertEquals("hello", Resource.deserializeObject(buffer));
  }


  @Test
  public void testInvokeMethod_shouldAbleToInvokeMethodWithTypeInference(){
    // TODO ..由此可见 Resource的invokeMethod就是用来反射执行资源对象（执行结果、状态等都是资源对象）的某些方法的，跟普通的反射工具类没啥区别
    Resource r = new Resource(null, new ResourceId("pool1", "name1"), "object");
    assertEquals("ect", r.invokeMethod("substring",new Class[]{int.class}, new Object[]{3}));
    assertEquals("obj", r.invokeMethod("substring",new Class[]{int.class,int.class}, new Object[]{0,3}));
    assertEquals(true, r.invokeMethod("startsWith",new Class[]{String.class}, new Object[]{"obj"}));

    assertEquals(2, r.invokeMethod("indexOf",new Class[]{int.class}, new Object[]{'j'}));
    assertEquals(4, r.invokeMethod("indexOf",new Class[]{String.class,int.class}, new Object[]{"ct",3}));

  }
}
