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

package org.apache.zeppelin.notebook.repo;

import com.aliyun.oss.ClientBuilderConfiguration;
import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.common.comm.Protocol;
import com.aliyun.oss.model.*;
import com.google.common.collect.Lists;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.zeppelin.conf.ZeppelinConfiguration;
import org.apache.zeppelin.conf.ZeppelinConfiguration.ConfVars;
import org.apache.zeppelin.notebook.Note;
import org.apache.zeppelin.notebook.NoteInfo;
import org.apache.zeppelin.user.AuthenticationInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Backend for storing Notebooks on OSS
 */
public class OSSNotebookRepo implements NotebookRepo {
  private static final Logger LOG = LoggerFactory.getLogger(OSSNotebookRepo.class);

  // OSS SDK ：https://www.alibabacloud.com/help/zh/doc-detail/32010.htm
  private final OSS ossClient;
  private final String bucketName;
  private final String user;
  private final ZeppelinConfiguration conf;

  public OSSNotebookRepo(ZeppelinConfiguration conf) throws IOException {
    this.conf = conf;
    bucketName = conf.getS3BucketName();
    user = conf.getS3User();
    String accessKeyId = conf.getOSSAccessKeyId();
    String accessKeySecret = conf.getOSSAccessKeySecret();
    String stsToken = conf.getOssStsToken();
    String endpoint = conf.getOSSEndpoint();

    ossClient = new OSSClientBuilder()
            .build(endpoint,accessKeyId,accessKeySecret,stsToken,createClientConfiguration());
  }


  /**
   * Create Aliyun client configuration and return it.
   * @return Aliyun client configuration
   */
  private ClientBuilderConfiguration createClientConfiguration() {

    ClientBuilderConfiguration config = new ClientBuilderConfiguration();
    // 设置OSSClient允许打开的最大HTTP连接数，默认为1024个。
    config.setMaxConnections(1024);
    // 设置Socket层传输数据的超时时间，默认为50000毫秒。
    config.setSocketTimeout(50000);
    // 设置建立连接的超时时间，默认为50000毫秒。
    config.setConnectionTimeout(50000);
    // 设置从连接池中获取连接的超时时间（单位：毫秒），默认不超时。
    config.setConnectionRequestTimeout(-1);
    // 设置连接空闲超时时间。超时则关闭连接，默认为60000毫秒。
    config.setIdleConnectionTime(60000);
    // 设置失败请求重试次数，默认为3次。
    config.setMaxErrorRetry(5);
    // 设置是否支持将自定义域名作为Endpoint，默认支持。
    config.setSupportCname(false);
    // 设置是否开启二级域名的访问方式，默认不开启。
    config.setSLDEnabled(false);
    // 设置连接OSS所使用的协议（HTTP/HTTPS），默认为HTTP。
    config.setProtocol(Protocol.HTTP);
    // 设置用户代理，指HTTP的User-Agent头，默认为aliyun-sdk-java。
    config.setUserAgent("aliyun-sdk-java");
    // 设置代理服务器端口。
//    config.setProxyHost("<yourProxyHost>");
    // 设置代理服务器验证的用户名。
//    config.setProxyUsername("<yourProxyUserName>");
    // 设置代理服务器验证的密码。
//    config.setProxyPassword("<yourProxyPassword>");
    return config;
  }

  @Override
  public List<NoteInfo> list(AuthenticationInfo subject) throws IOException {
    List<NoteInfo> infos = new LinkedList<>();
    NoteInfo info;
    try {

      ListObjectsRequest listObjectsRequest = new ListObjectsRequest(bucketName)
              .withPrefix(user + "/notebook");
      ObjectListing objectListing;
      do {
        objectListing = ossClient.listObjects(listObjectsRequest);
        for (OSSObjectSummary objectSummary : objectListing.getObjectSummaries()) {
          if (objectSummary.getKey().endsWith("note.json")) {
            info = getNoteInfo(objectSummary.getKey());
            if (info != null) {
              infos.add(info);
            }
          }
        }
        listObjectsRequest.setMarker(objectListing.getNextMarker());
      } while (objectListing.isTruncated());
    } catch (Exception e) {
      throw new IOException("Unable to list objects in OSS: " + e, e);
    }
    return infos;
  }

  private Note getNote(String key) throws IOException {
    OSSObject ossObject;
    try {
      ossObject = ossClient.getObject(new GetObjectRequest(bucketName, key));
    }
    catch (Exception ace) {
      throw new IOException("Unable to retrieve object from OSS: " + ace, ace);
    }

    try (InputStream ins = ossObject.getObjectContent()) {
      String json = IOUtils.toString(ins, conf.getString(ConfVars.ZEPPELIN_ENCODING));
      return Note.fromJson(json);
    }
  }

  private NoteInfo getNoteInfo(String key) throws IOException {
    Note note = getNote(key);
    return new NoteInfo(note);
  }

  @Override
  public Note get(String noteId, AuthenticationInfo subject) throws IOException {
    return getNote(user + "/" + "notebook" + "/" + noteId + "/" + "note.json");
  }

  @Override
  public void save(Note note, AuthenticationInfo subject) throws IOException {

    String json = note.toJson();
    String key = user + "/" + "notebook" + "/" + note.getId() + "/" + "note.json";

    File file = File.createTempFile("note", "json");
    try {
      Writer writer = new OutputStreamWriter(new FileOutputStream(file));
      writer.write(json);
      writer.close();

      PutObjectRequest putRequest = new PutObjectRequest(bucketName, key, file);


      ossClient.putObject(putRequest);
    }
    catch (Exception ace) {
      throw new IOException("Unable to store note in OSS: " + ace, ace);
    }
    finally {
      FileUtils.deleteQuietly(file);
    }
  }

  @Override
  public void remove(String noteId, AuthenticationInfo subject) throws IOException {
    String key = user + "/" + "notebook" + "/" + noteId;
    final ListObjectsRequest listObjectsRequest = new ListObjectsRequest(bucketName)
        .withPrefix(key);

    try {
      ObjectListing objects = ossClient.listObjects(listObjectsRequest);
      do {
        if (objects.getObjectSummaries().size() > 0) {
          List<String> keys = Lists.newArrayList();
          for (OSSObjectSummary objectSummary : objects.getObjectSummaries()) {
            keys.add(objectSummary.getKey());
          }
          DeleteObjectsRequest deleteObjectsRequest = new DeleteObjectsRequest(bucketName).withKeys(keys);
          ossClient.deleteObjects(deleteObjectsRequest);
        }
//        for (OSSObjectSummary objectSummary : objects.getObjectSummaries()) {
//          ossClient.deleteObject(bucketName, objectSummary.getKey());
//        }
      } while (objects.isTruncated());
    }
    catch (Exception ace) {
      throw new IOException("Unable to remove note in OSS: " + ace, ace);
    }
  }

  @Override
  public void close() {
    ossClient.shutdown();
  }

  @Override
  public List<NotebookRepoSettingsInfo> getSettings(AuthenticationInfo subject) {
    LOG.warn("Method not implemented");
    return Collections.emptyList();
  }

  @Override
  public void updateSettings(Map<String, String> settings, AuthenticationInfo subject) {
    LOG.warn("Method not implemented");
  }

}
