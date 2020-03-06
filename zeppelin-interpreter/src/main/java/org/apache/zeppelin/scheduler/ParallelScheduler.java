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

package org.apache.zeppelin.scheduler;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;

import org.apache.zeppelin.scheduler.Job.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Parallel scheduler runs submitted job concurrently.
 */
public class ParallelScheduler implements Scheduler {
  List<Job> queue = new LinkedList<>();
  List<Job> running = new LinkedList<>();
  private ExecutorService executor;
  private SchedulerListener listener;
  boolean terminate = false;
  private String name;
  private int maxConcurrency;

  static Logger LOGGER = LoggerFactory.getLogger(ParallelScheduler.class);

  public ParallelScheduler(String name, ExecutorService executor, SchedulerListener listener,
      int maxConcurrency) {
    this.name = name;
    this.executor = executor;
    this.listener = listener;
    // TODO 注意这里的最大并发是有问题的 如果zeppelin.scheduler.threadpool.size配置大于maxConcurrency的话
    //  会有大量线程被浪费，如果zeppelin.scheduler.threadpool.size配置小于于maxConcurrency又达不到最大并发度
    //  原因是线程池大小和允许的并发数不一定一致，因为是通过两个参数控制的,对于这个问题，0.9.x新版本的ParallelScheduler通过单独构建
    //  一个size为maxConcurrency的线程池来控制当前解释器的并发数，这样就保证了最大并发是一定能达到maxConcurrency且不会有线程浪费。
    //
    //  TODO 这里要注意下：之前的分析错了（master分支和0.8.x分支之前关于调度器的分析都忽略掉，因为不正确）
    //    - FIFO调度器和并行调度器应该是用在解释器进程中的，用于在解释器进程中提交/执行任务，它们和zeppelin服务不在同一个JVM，
    //      所以它们资源是不共享的（和zeppelin服务不共享，解释器进程间也不共享）：RemoteInterpreterServer.interpret里面会初始化
    //      对应解释器的调度器
    //    - RemoteScheduler是运行在zeppelin服务中用于和各个解释器进行交互的，而且一个session会构建一个RemoteScheduler,但是虽然是多个
    //     RemoteScheduler,但是由于它们在同一个JVM里面，而且SchedulerFactory又是单例的，所以这些RemoteScheduler是共享一个线程池的
    //
    this.maxConcurrency = maxConcurrency;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public Collection<Job> getJobsWaiting() {
    List<Job> ret = new LinkedList<>();
    synchronized (queue) {
      for (Job job : queue) {
        ret.add(job);
      }
    }
    return ret;
  }

  @Override
  public Job removeFromWaitingQueue(String jobId) {
    synchronized (queue) {
      Iterator<Job> it = queue.iterator();
      while (it.hasNext()) {
        Job job = it.next();
        if (job.getId().equals(jobId)) {
          it.remove();
          return job;
        }
      }
    }
    return null;
  }

  @Override
  public Collection<Job> getJobsRunning() {
    List<Job> ret = new LinkedList<>();
    synchronized (queue) {
      for (Job job : running) {
        ret.add(job);
      }
    }
    return ret;
  }



  @Override
  public void submit(Job job) {
    job.setStatus(Status.PENDING);
    synchronized (queue) {
      queue.add(job);
      queue.notify();
    }
  }

  @Override
  public void run() {
    while (terminate == false) {
      Job job = null;
      synchronized (queue) {
        if (running.size() >= maxConcurrency || queue.isEmpty() == true) {
          try {
            queue.wait(500);
          } catch (InterruptedException e) {
            LOGGER.error("Exception in MockInterpreterAngular while interpret queue.wait", e);
          }
          continue;
        }

        job = queue.remove(0);
        running.add(job);
      }
      Scheduler scheduler = this;

      executor.execute(new JobRunner(scheduler, job));
    }
  }

  public void setMaxConcurrency(int maxConcurrency) {
    this.maxConcurrency = maxConcurrency;
    synchronized (queue) {
      queue.notify();
    }
  }

  private class JobRunner implements Runnable {
    private Scheduler scheduler;
    private Job job;

    public JobRunner(Scheduler scheduler, Job job) {
      this.scheduler = scheduler;
      this.job = job;
    }

    @Override
    public void run() {
      if (job.isAborted()) {
        job.setStatus(Status.ABORT);
        job.aborted = false;

        synchronized (queue) {
          running.remove(job);
          queue.notify();
        }

        return;
      }

      job.setStatus(Status.RUNNING);
      if (listener != null) {
        listener.jobStarted(scheduler, job);
      }
      job.run();
      if (job.isAborted()) {
        job.setStatus(Status.ABORT);
      } else {
        if (job.getException() != null) {
          job.setStatus(Status.ERROR);
        } else {
          job.setStatus(Status.FINISHED);
        }
      }

      if (listener != null) {
        listener.jobFinished(scheduler, job);
      }

      // reset aborted flag to allow retry
      job.aborted = false;
      synchronized (queue) {
        running.remove(job);
        queue.notify();
      }
    }
  }


  @Override
  public void stop() {
    terminate = true;
    synchronized (queue) {
      queue.notify();
    }
  }

}
