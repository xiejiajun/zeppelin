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

/**
 * TODO 目前看来ResourcePool是用来报错ZeppelinContext中定义的变量等信息的
 *   http://zeppelin.apache.org/docs/0.9.0-SNAPSHOT/interpreter/shell.html
 *   http://zeppelin.apache.org/docs/0.9.0-preview2/interpreter/jdbc.html#object-interpolation
 *    - scala代码段定义变量: z.put("dataFileName", "members-list-003.parquet")
 *    - sh解释器使用变量(前提是zeppelin.shell.interpolation=true): echo {dataFileName}
 *    - jdbc解释器使用变量(前提是zeppelin.jdbc.interpolation为true):
 *      - select * from patents_list where df_name = '{dataFileName}' and filing_date like '2015-%'
 *    - ResourcePool的上层APIZeppelin-Context:
 *      - http://zeppelin.apache.org/docs/0.8.2/usage/other_features/zeppelin_context.html
 * distributed resource pool
 */
public class DistributedResourcePool extends LocalResourcePool {

  private final ResourcePoolConnector connector;

  public DistributedResourcePool(String id, ResourcePoolConnector connector) {
    super(id);
    this.connector = connector;
  }

  @Override
  public Resource get(String name) {
    return get(name, true);
  }

  @Override
  public Resource get(String noteId, String paragraphId, String name) {
    return get(noteId, paragraphId, name, true);
  }

  /**
   * get resource by name.
   * @param name
   * @param remote false only return from local resource
   * @return null if resource not found.
   */
  public Resource get(String name, boolean remote) {
    // try local first
    Resource resource = super.get(name);
    if (resource != null) {
      return resource;
    }

    if (remote) {
      ResourceSet resources = connector.getAllResources().filterByName(name);
      if (resources.isEmpty()) {
        return null;
      } else {
        // TODO(zjffdu) just assume there's no dupicated resources with the same name, but
        // this assumption is false
        return resources.get(0);
      }
    } else {
      return null;
    }
  }

  /**
   * get resource by name.
   * @param name
   * @param remote false only return from local resource
   * @return null if resource not found.
   */
  public Resource get(String noteId, String paragraphId, String name, boolean remote) {
    // try local first
    Resource resource = super.get(noteId, paragraphId, name);
    if (resource != null) {
      return resource;
    }

    if (remote) {
      ResourceSet resources = connector.getAllResources()
          .filterByNoteId(noteId)
          .filterByParagraphId(paragraphId)
          .filterByName(name);

      if (resources.isEmpty()) {
        return null;
      } else {
        return resources.get(0);
      }
    } else {
      return null;
    }
  }

  @Override
  public ResourceSet getAll() {
    return getAll(true);
  }

  /**
   * Get all resource from the pool
   * @param remote false only return local resource
   * @return
   */
  public ResourceSet getAll(boolean remote) {
    ResourceSet all = super.getAll();
    if (remote) {
      all.addAll(connector.getAllResources());
    }
    return all;
  }
}
