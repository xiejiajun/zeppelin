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

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.zeppelin.conf.ZeppelinConfiguration;
import org.apache.zeppelin.notebook.Note;
import org.apache.zeppelin.user.AuthenticationInfo;
import org.eclipse.jgit.api.*;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.dircache.DirCache;
import org.eclipse.jgit.transport.URIish;
import org.eclipse.jgit.transport.UsernamePasswordCredentialsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * TODO GitHubNotebookRepo是GitNotebookRepo的子类，只重写了用于做版本管理的checkpoint方法，所以zeppelin上新建的notebook只有
 *  手动commit后git仓库才看得到
 *  由于没有重写remove方法，所以删除操作只是删除本地的文件 并没有同步删掉git上的
 *
 *  TODO 下面的save和remove方法是我二开加上去的
 * GitHub integration to store notebooks in a GitHub repository.
 * It uses the same simple logic implemented in @see
 * {@link org.apache.zeppelin.notebook.repo.GitNotebookRepo}
 *
 * The logic for updating the local repository from the remote repository is the following:
 * - When the <code>GitHubNotebookRepo</code> is initialized
 * - When pushing the changes to the remote repository
 *
 * The logic for updating the remote repository on GitHub from local repository is the following:
 * - When commit the changes (saving the notebook)
 */
public class GitHubNotebookRepo extends GitNotebookRepo {

  private final String ASTERISK = "*";

  private final String POINT = ".";

  private static final Logger LOG = LoggerFactory.getLogger(GitNotebookRepo.class);
  private ZeppelinConfiguration zeppelinConfiguration;
  private Git git;

  private AtomicInteger removedNotebookCnt = new AtomicInteger(0);
  private AtomicInteger saveNotebookCnt =  new AtomicInteger(0);

  private Integer batchSaveThreshold = 10 * 2;
  private Integer batchRemoveThreshold = 50;

  private RemoteRepoRefresher remoteRepoRefresher;

  public GitHubNotebookRepo(ZeppelinConfiguration conf) throws IOException {
    super(conf);

    this.git = super.getGit();
    this.zeppelinConfiguration = conf;

    this.remoteRepoRefresher = new RemoteRepoRefresher(this);

    configureRemoteStream();
    pullFromRemoteStream();
  }

  @Override
  public Revision checkpoint(String pattern, String commitMessage, AuthenticationInfo subject) {
    Revision revision = super.checkpoint(pattern, commitMessage, subject);

    updateRemoteStream();

    return revision;
  }

  private void configureRemoteStream() {
    try {
      LOG.debug("Setting up remote stream");
      RemoteAddCommand remoteAddCommand = git.remoteAdd();
      remoteAddCommand.setName(zeppelinConfiguration.getZeppelinNotebookGitRemoteOrigin());
      remoteAddCommand.setUri(new URIish(zeppelinConfiguration.getZeppelinNotebookGitURL()));
      remoteAddCommand.call();
    } catch (GitAPIException e) {
      LOG.error("Error configuring GitHub", e);
    } catch (URISyntaxException e) {
      LOG.error("Error in GitHub URL provided", e);
    }
  }

  private void updateRemoteStream() {
    LOG.debug("Updating remote stream");

    pullFromRemoteStream();
    pushToRemoteSteam();
  }

  private void pullFromRemoteStream() {
    try {
      LOG.debug("Pull latest changed from remote stream");
      PullCommand pullCommand = git.pull();
      pullCommand.setCredentialsProvider(
        new UsernamePasswordCredentialsProvider(
          zeppelinConfiguration.getZeppelinNotebookGitUsername(),
          zeppelinConfiguration.getZeppelinNotebookGitAccessToken()
        )
      );

      pullCommand.call();

    } catch (GitAPIException e) {
      LOG.error("Error when pulling latest changes from remote repository", e);
    }
  }

  private void pushToRemoteSteam() {
    try {
      LOG.debug("Push latest changed from remote stream");
      PushCommand pushCommand = git.push();
      pushCommand.setCredentialsProvider(
        new UsernamePasswordCredentialsProvider(
          zeppelinConfiguration.getZeppelinNotebookGitUsername(),
          zeppelinConfiguration.getZeppelinNotebookGitAccessToken()
        )
      );

      pushCommand.call();
    } catch (GitAPIException e) {
      LOG.error("Error when pushing latest changes from remote repository", e);
    }
  }


  @Override
  public synchronized void save(Note note, AuthenticationInfo subject) throws IOException {
    LOG.info("save note {} to git origin repo",note.getId());
    super.save(note, subject);
//    updateRemoteRepo(note.getId());
    batchSaveNoteToRemoteRepo(note.getId());
  }

  @Override
  public void remove(String noteId, AuthenticationInfo subject) throws IOException {
    LOG.info("remove note {} from git origin repo",noteId);
    super.remove(noteId,subject);
    try {
//      deleteFileFromRemoteRepo(noteId);
      batchRemoveNoteFromRemoteRepo(noteId);
    } catch (GitAPIException e) {
      LOG.error(e.getMessage(),e);
    }
  }

  @Override
  public void close() {
    super.close();
    remoteRepoRefresher.close();
  }



    /**
     * 将本地改动更新到远程仓库
     * @param noteId
     */
  private void updateRemoteRepo(String noteId){
    LOG.debug("git add {}", noteId);
    try {
      String commitMessage = add(noteId);
      // TODO git commit命令
      git.commit().setMessage(commitMessage).call();
      // TODO push到远程仓库
      pushToRemoteSteam();
    } catch (GitAPIException e) {
      LOG.error(e.getMessage(),e);
    }
  }

  /**
   * TODO 考虑换成batch方式进行批量删除，不然效率太低，git服务也扛不住
   * 确保能完全删除远程仓库上对应文件
   * @param noteId
   */
  private void deleteFileFromRemoteRepo(String noteId) throws GitAPIException {
    if (StringUtils.isNotBlank(noteId) && !noteId.contains(POINT) && !noteId.contains(ASTERISK)){
      git.rm().addFilepattern(noteId).call();
    }
    updateRemoteRepo(noteId);
  }

  /**
   * 添加改动
   * @param noteId
   * @return
   */
  private String add(String noteId) throws GitAPIException {
    if (StringUtils.isBlank(noteId)){
      return  "no change";
    }
    // TODO git add命令
    DirCache added = git.add().addFilepattern(noteId).call();
    String commitMessage = String.format("sync notebook %s to origin",noteId);
    LOG.debug("git commit -m '{}' :{} changes are about to be commited", commitMessage, added.getEntryCount());
    return commitMessage;

  }


  /**
   * 批量更新远程仓库
   * @param commitMsg
   */
  private void batchUpdateRemoteRepo(String commitMsg){
    try {
      Status status = git.status().call();
      if (CollectionUtils.isNotEmpty(status.getChanged())
              || CollectionUtils.isNotEmpty(status.getAdded())
              || CollectionUtils.isNotEmpty(status.getRemoved())) {
        // TODO git commit命令
        git.commit().setMessage(commitMsg).call();
        // TODO push到远程仓库
        pushToRemoteSteam();
      }
    } catch (GitAPIException e) {
      LOG.error(e.getMessage(),e);
    }
  }


  /**
   * 批量触发保存note方法实现
   * @param noteId
   */
  private void batchSaveNoteToRemoteRepo(String noteId){
    try {
      add(noteId);
      // 每5分钟或者批次到达batchSaveThreshold时触发一次提交
      int addSize = saveNotebookCnt.incrementAndGet();
      if ( addSize >= batchSaveThreshold){
        batchUpdateRemoteRepo("batch save notebook to origin");
        saveNotebookCnt.addAndGet(-1 * addSize);
      }
    } catch (GitAPIException e) {
      LOG.error(e.getMessage(),e);
    }
  }

  /**
   * 批量从远程仓库删除note
   * @param noteId
   */
  private void batchRemoveNoteFromRemoteRepo(String noteId) throws GitAPIException {
    if (StringUtils.isNotBlank(noteId) && !noteId.contains(POINT) && !noteId.contains(ASTERISK)){
      git.rm().addFilepattern(noteId).call();
    }
    add(noteId);
    int removeSize = removedNotebookCnt.incrementAndGet();
    if ( removeSize >= batchRemoveThreshold){
      batchUpdateRemoteRepo("batch remove notebook from origin");
      removedNotebookCnt.addAndGet(-1 * removeSize);
    }

  }


  static class RemoteRepoRefresher {

    private ScheduledExecutorService executors;

    RemoteRepoRefresher(final GitHubNotebookRepo gitHubNotebookRepo){
      this.executors = Executors.newSingleThreadScheduledExecutor();
      // TODO 5分钟尝试往远程仓库推一次代码
      this.executors.scheduleWithFixedDelay(()->{
          gitHubNotebookRepo.batchUpdateRemoteRepo("定时推送");
      },5, 5,TimeUnit.MINUTES);
    }

    public void close(){
      executors.shutdownNow();
    }
  }

}
