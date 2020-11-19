/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zeppelin.flink

import java.io._

import org.apache.flink.annotation.Internal
import org.apache.flink.client.cli.{CliFrontend, CliFrontendParser, CustomCommandLine}
import org.apache.flink.client.deployment.DefaultClusterClientServiceLoader
import org.apache.flink.client.deployment.executors.RemoteExecutor
import org.apache.flink.client.program.{ClusterClient, MiniClusterClient}
import org.apache.flink.configuration._
import org.apache.flink.runtime.minicluster.{MiniCluster, MiniClusterConfiguration}
import org.apache.flink.yarn.executors.YarnSessionClusterExecutor

import scala.collection.mutable.ArrayBuffer

/**
 * Copy from flink, because we need to customize it to make sure
 * it work with multiple versions of flink.
 */
object FlinkShell {

  object ExecutionMode extends Enumeration {
    val UNDEFINED, LOCAL, REMOTE, YARN = Value
  }

  /** Configuration object */
  case class Config(
    host: Option[String] = None,
    port: Option[Int] = None,
    externalJars: Option[Array[String]] = None,
    executionMode: ExecutionMode.Value = ExecutionMode.UNDEFINED,
    yarnConfig: Option[YarnConfig] = None,
    configDir: Option[String] = None
  )

  /** YARN configuration object */
  case class YarnConfig(
    jobManagerMemory: Option[String] = None,
    name: Option[String] = None,
    queue: Option[String] = None,
    slots: Option[Int] = None,
    taskManagerMemory: Option[String] = None
  )

  /** Buffered reader to substitute input in test */
  var bufferedReader: Option[BufferedReader] = None

  @Internal def ensureYarnConfig(config: Config) = config.yarnConfig match {
    case Some(yarnConfig) => yarnConfig
    case None => YarnConfig()
  }

  private def getConfigDir(config: Config) = {
    config.configDir.getOrElse(CliFrontend.getConfigurationDirectoryFromEnv)
  }

  @Internal def fetchConnectionInfo(
      config: Config,
      flinkConfig: Configuration,
      flinkShims: FlinkShims): (Configuration, Option[ClusterClient[_]]) = {

    config.executionMode match {
        // TODO 启动LOCAL模式的Flink进程(这里的Local模式其实是为伪布式，会启动一个MiniCluster),
      case ExecutionMode.LOCAL => createLocalClusterAndConfig(flinkConfig)
        // TODO 连接Standalone模式的集群
      case ExecutionMode.REMOTE => createRemoteConfig(config, flinkConfig)
        // TODO 在Yarn上启动Flink（类似yarn-session模式)
      case ExecutionMode.YARN => createYarnClusterIfNeededAndGetConfig(config, flinkConfig, flinkShims)
      case ExecutionMode.UNDEFINED => // Wrong input
        throw new IllegalArgumentException("please specify execution mode:\n" +
          "[local | remote <host> <port> | yarn]")
    }
  }

  private def createYarnClusterIfNeededAndGetConfig(config: Config, flinkConfig: Configuration, flinkShims: FlinkShims) = {
    // TODO 指定ATTACHED模式是为了作业提交后保持交互，即交互模式(默认为detached即分离模式)
    flinkConfig.setBoolean(DeploymentOptions.ATTACHED, true)

    val (clusterConfig, clusterClient) = config.yarnConfig match {
        // TODO  启动yarn-session集群
      case Some(_) => deployNewYarnCluster(config, flinkConfig, flinkShims)
      case None => (flinkConfig, None)
    }

    // workaround for FLINK-17788, otherwise it won't work with flink 1.10.1 which has been released.
    // TODO 设置为yarn-session模式
    flinkConfig.set(DeploymentOptions.TARGET, YarnSessionClusterExecutor.NAME)

    val (effectiveConfig, _) = clusterClient match {
        // TODO 获取远程配置？这里面的CliFrontend应该会获取到FlinkYarnSessionCli, 它的applyCommandLineOptionsToConfiguration方法返回的
        //  配置待遇yarn-session的appId等信息
      case Some(_) => fetchDeployedYarnClusterInfo(config, clusterConfig, "yarn-cluster", flinkShims)
      case None => fetchDeployedYarnClusterInfo(config, clusterConfig, "default", flinkShims)
    }

    println("Configuration: " + effectiveConfig)

    (effectiveConfig, clusterClient)
  }

  private def deployNewYarnCluster(config: Config, flinkConfig: Configuration, flinkShims: FlinkShims) = {
    val effectiveConfig = new Configuration(flinkConfig)
    // TODO 启动一个ATTACHED模式的yarn-cluster所以也相当于yarn-session模式
    val args = parseArgList(config, "yarn-cluster")

    val configurationDirectory = getConfigDir(config)

    val frontend = new CliFrontend(
      effectiveConfig,
      CliFrontend.loadCustomCommandLines(effectiveConfig, configurationDirectory))

    val commandOptions = CliFrontendParser.getRunCommandOptions
    val commandLineOptions = CliFrontendParser.mergeOptions(commandOptions,
      frontend.getCustomCommandLineOptions)
    // TODO 处理parseArgList生成的提交参数
    val commandLine = CliFrontendParser.parse(commandLineOptions, args, true)

    val customCLI = flinkShims.getCustomCli(frontend, commandLine).asInstanceOf[CustomCommandLine]
    val executorConfig = customCLI.applyCommandLineOptionsToConfiguration(commandLine)

    val serviceLoader = new DefaultClusterClientServiceLoader
    val clientFactory = serviceLoader.getClusterClientFactory(executorConfig)
    val clusterDescriptor = clientFactory.createClusterDescriptor(executorConfig)
    val clusterSpecification = clientFactory.getClusterSpecification(executorConfig)

    val clusterClient = try {
      clusterDescriptor
        // TODO 在Yarn上创建集群
        .deploySessionCluster(clusterSpecification)
        .getClusterClient
    } finally {
      executorConfig.set(DeploymentOptions.TARGET, "yarn-session")
      clusterDescriptor.close()
    }

    (executorConfig, Some(clusterClient))
  }

  private def fetchDeployedYarnClusterInfo(
      config: Config,
      flinkConfig: Configuration,
      mode: String,
      flinkShims: FlinkShims) = {

    val effectiveConfig = new Configuration(flinkConfig)
    val args = parseArgList(config, mode)

    val configurationDirectory = getConfigDir(config)

    val frontend = new CliFrontend(
      effectiveConfig,
      CliFrontend.loadCustomCommandLines(effectiveConfig, configurationDirectory))

    val commandOptions = CliFrontendParser.getRunCommandOptions
    val commandLineOptions = CliFrontendParser.mergeOptions(commandOptions,
      frontend.getCustomCommandLineOptions)
    val commandLine = CliFrontendParser.parse(commandLineOptions, args, true)

    val customCLI = flinkShims.getCustomCli(frontend, commandLine).asInstanceOf[CustomCommandLine]
    val executorConfig = customCLI.applyCommandLineOptionsToConfiguration(commandLine);

    (executorConfig, None)
  }

  def parseArgList(config: Config, mode: String): Array[String] = {
    val args = if (mode == "default") {
      ArrayBuffer[String]()
    } else {
      ArrayBuffer[String]("-m", mode)
    }

    config.yarnConfig match {
      case Some(yarnConfig) =>
        yarnConfig.jobManagerMemory.foreach((jmMem) => args ++= Seq("-yjm", jmMem.toString))
        yarnConfig.taskManagerMemory.foreach((tmMem) => args ++= Seq("-ytm", tmMem.toString))
        yarnConfig.name.foreach((name) => args ++= Seq("-ynm", name.toString))
        yarnConfig.queue.foreach((queue) => args ++= Seq("-yqu", queue.toString))
        yarnConfig.slots.foreach((slots) => args ++= Seq("-ys", slots.toString))
        args.toArray
      case None => args.toArray
    }
  }

  private def createRemoteConfig(
      config: Config,
      flinkConfig: Configuration): (Configuration, None.type) = {

    if (config.host.isEmpty || config.port.isEmpty) {
      throw new IllegalArgumentException("<host> or <port> is not specified!")
    }

    val effectiveConfig = new Configuration(flinkConfig)
    // TODO 将standalone的连接信息放到effectiveConfig, 方便创建FlinkILoop时使用
    setJobManagerInfoToConfig(effectiveConfig, config.host.get, config.port.get)
    effectiveConfig.set(DeploymentOptions.TARGET, RemoteExecutor.NAME)
    effectiveConfig.setBoolean(DeploymentOptions.ATTACHED, true)

    (effectiveConfig, None)
  }

  private def createLocalClusterAndConfig(flinkConfig: Configuration) = {
    val config = new Configuration(flinkConfig)
    config.setInteger(JobManagerOptions.PORT, 0)

    val cluster = createLocalCluster(config)
    val port = cluster.getRestAddress.get.getPort

    // TODO 将standalone的连接信息放到effectiveConfig, 方便创建FlinkILoop时使用
    setJobManagerInfoToConfig(config, "localhost", port)
    config.set(DeploymentOptions.TARGET, RemoteExecutor.NAME)
    config.setBoolean(DeploymentOptions.ATTACHED, true)

    println(s"\nStarting local Flink cluster (host: localhost, port: ${port}).\n")

    val clusterClient = new MiniClusterClient(config, cluster)
    (config, Some(clusterClient))
  }

  private def createLocalCluster(flinkConfig: Configuration) = {

    val numTaskManagers = flinkConfig.getInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER,
      ConfigConstants.DEFAULT_LOCAL_NUMBER_TASK_MANAGER)
    val numSlotsPerTaskManager = flinkConfig.getInteger(TaskManagerOptions.NUM_TASK_SLOTS)

    val miniClusterConfig = new MiniClusterConfiguration.Builder()
      .setConfiguration(flinkConfig)
      .setNumSlotsPerTaskManager(numSlotsPerTaskManager)
      .setNumTaskManagers(numTaskManagers)
      .build()

    val cluster = new MiniCluster(miniClusterConfig)
    cluster.start()
    cluster
  }

  private def setJobManagerInfoToConfig(
      config: Configuration,
      host: String, port: Integer): Unit = {

    config.setString(JobManagerOptions.ADDRESS, host)
    config.setInteger(JobManagerOptions.PORT, port)

    config.setString(RestOptions.ADDRESS, host)
    config.setInteger(RestOptions.PORT, port)
  }
}
