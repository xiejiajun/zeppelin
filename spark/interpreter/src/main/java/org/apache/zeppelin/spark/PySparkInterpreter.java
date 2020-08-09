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

package org.apache.zeppelin.spark;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.zeppelin.interpreter.ZeppelinContext;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterException;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.interpreter.util.InterpreterOutputStream;
import org.apache.zeppelin.python.IPythonInterpreter;
import org.apache.zeppelin.python.PythonInterpreter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

/**
 *  Interpreter for PySpark, it is the first implementation of interpreter for PySpark, so with less
 *  features compared to IPySparkInterpreter, but requires less prerequisites than
 *  IPySparkInterpreter, only python is required.
 */
public class PySparkInterpreter extends PythonInterpreter {

  private static Logger LOGGER = LoggerFactory.getLogger(PySparkInterpreter.class);

  private SparkInterpreter sparkInterpreter;

  public PySparkInterpreter(Properties property) {
    super(property);
    this.useBuiltinPy4j = false;
  }

  @Override
  public void open() throws InterpreterException {
    setProperty("zeppelin.python.useIPython", getProperty("zeppelin.pyspark.useIPython", "true"));
    URL [] urls = new URL[0];
    List<URL> urlList = new LinkedList<>();
    String localRepo = getProperty("zeppelin.interpreter.localRepo");
    if (localRepo != null) {
      File localRepoDir = new File(localRepo);
      if (localRepoDir.exists()) {
        File[] files = localRepoDir.listFiles();
        if (files != null) {
          for (File f : files) {
            try {
              urlList.add(f.toURI().toURL());
            } catch (MalformedURLException e) {
              LOGGER.error("Error", e);
            }
          }
        }
      }
    }

    urls = urlList.toArray(urls);
    ClassLoader oldCl = Thread.currentThread().getContextClassLoader();
    try {
      URLClassLoader newCl = new URLClassLoader(urls, oldCl);
      Thread.currentThread().setContextClassLoader(newCl);
      // must create spark interpreter after ClassLoader is set, otherwise the additional jars
      // can not be loaded by spark repl.
      this.sparkInterpreter = getInterpreterInTheSameSessionByClassName(SparkInterpreter.class);
      setProperty("zeppelin.py4j.useAuth",
          sparkInterpreter.getSparkVersion().isSecretSocketSupported() + "");
      // create Python Process and JVM gateway
      super.open();
    } finally {
      Thread.currentThread().setContextClassLoader(oldCl);
    }

    if (!useIPython()) {
      // Initialize Spark in Python Process
      try {
        bootstrapInterpreter("python/zeppelin_pyspark.py");
      } catch (IOException e) {
        LOGGER.error("Fail to bootstrap pyspark", e);
        throw new InterpreterException("Fail to bootstrap pyspark", e);
      }
    }
  }

  @Override
  public void close() throws InterpreterException {
    super.close();
    if (sparkInterpreter != null) {
      sparkInterpreter.close();
    }
  }

  @Override
  protected IPythonInterpreter getIPythonInterpreter() throws InterpreterException {
    return getInterpreterInTheSameSessionByClassName(IPySparkInterpreter.class, false);
  }

  @Override
  protected ZeppelinContext createZeppelinContext() {
    return sparkInterpreter.getZeppelinContext();
  }

  @Override
  public InterpreterResult interpret(String st, InterpreterContext context)
      throws InterpreterException {
    // redirect java stdout/stdout to interpreter output. Because pyspark may call java code.
    PrintStream originalStdout = System.out;
    PrintStream originalStderr = System.err;
    try {
      System.setOut(new PrintStream(context.out));
      System.setErr(new PrintStream(context.out));
      Utils.printDeprecateMessage(sparkInterpreter.getSparkVersion(), context, properties);
      // TODO Zeppelin PySpark的交互式执行流程：
      //  PySparkInterpreter流程：
      //  RemoteInterpreter.interpret -> PySparkInterpreter.interpret -> PythonInterpreter.interpreter
      //  -> PythonInterpreter.callPython -> statementSetNotifier.notify() -> PythonInterpreter.getStatements
      //  -> zeppelin_python.py里面req = intp.getStatements() -> 又由于PySparkInterpreter.open里面的bootstrapInterpreter
      //  -> 启动时通过zeppelin_python.py执行了zeppelin_pyspark.py脚本而且顺带调用了__zeppelin__._displayhook()用来处理输出格式
      //  -> 然后在zeppelin_python.py中通过python的exec函数执行用户写的pyspark代码（由于之前open的时候通过zeppelin_python.py中执行过
      //  -> zeppelin_pyspark.py代码了，所以用户需要的pyspark依赖都已经引入，gateway复用了zeppelin_python.py里面初始化好的那个，所以
      //  交互的时候用的是Zeppelin这边PythonInterpreter里面初始化的GateWayServer）-> pyspark需要调用JavaSparkContext执行操作时
      //  最终会引用SparkInterpreter中sc去执行，从而到达pyspark、Scala或者sparkSql都复用同一个SparkSession或者SparkContext的目的
      //  （注意：Executor端使用JavaSparkContext执行某些方法参数为python代码的操作时还会通过pipeline在executor本地启动python进程来执行对应的python
      //   代码，然后将python执行结果作为入参来执行。这就要注意了，如果Zeppelin这边注入的某些会影响Executor的Python环境变量在Executor上
      //   不可用的话可能会导致任务失败,分析Spark源码得出pythonExec是从环境变量PYSPARK_PYTHON获取的【org.apache.spark.deploy.PythonRunner和
      //   pyspark/context.py中的SparkContext._do_init里面都有相关初始化代码】），所以zeppelin_pyspark.py里面
      //   `sc = _zsc_ = SparkContext(jsc=jsc, gateway=gateway, conf=conf)`这行代码会通过PYSPARK_PYTHON获取可执行环境
      //
      // TODO IPySparkInterpreter流程：
      //  RemoteInterpreter.interpret -> PySparkInterpreter.interpret  -> PythonInterpreter.interpreter
      //  -> IPySparkInterpreter.interpreter(这里的open方法最终是通过父类IPythonInterpreter的open方法初始化的，只不过通过
      //  setAdditionalPythonInitFile("python/zeppelin_ipyspark.py")将启动Python进程的脚本替换成了ipyspark相关的【注意这里和
      //  PySparkInterpreter的区别，他没有像PySparkInterpreter一样是通过在已有的脚本里面执行zeppelin_pyspark.py文件，也就是
      //  zeppelin_pyspark.py是一个不完整的脚本，而zeppelin_ipyspark.py是一个完整脚本，而且在后面他会将zeppelin_ipython.py和zeppelin_context.py在
      //  zeppelin_ipyspark.py进程里面执行从而时zeppelin_ipyspark进程里面有一个能和IpythonInterpreter中的gateWay Server交互的客户端】)
      //  -> AbstractInterpreter.interpreter -> JupyterKernelInterpreter.internalInterpret() -> jupyterKernelClient.stream_execute
      //  -> kernel_server.py -> 最终执行用户的python脚本（因为zeppelin_ipyspark.py已经import了pyspark相关依赖而且也绑定了IPythonInterpreter中
      //  启动的Py4j GateWay Server, 所以用户代码执行是已经可以通过IPythonInterpreter获取公共的JavaSparkContext来使用了)

      // TODO zeppelin_pyspark.py或者zeppelin_ipyspark.py里面`jsc = intp.getJavaSparkContext()`将会调用PySparkInterpreter或者
      //  IPySparkInterpreter里面的getJavaSparkContext方法，该方法返回的JavaSparkContext是基于SparkInterpreter里面的SparkContext
      //  包装的，所以底层调用的都是同一个SCALA SparkContext实例
      return super.interpret(st, context);
    } finally {
      System.setOut(originalStdout);
      System.setErr(originalStderr);
    }
  }

  @Override
  protected void preCallPython(InterpreterContext context) {
    String jobGroup = Utils.buildJobGroupId(context);
    String jobDesc = Utils.buildJobDesc(context);
    callPython(new PythonInterpretRequest(
        String.format("if 'sc' in locals():\n\tsc.setJobGroup('%s', '%s')", jobGroup, jobDesc),
        false, false));

    String pool = "None";
    if (context.getLocalProperties().containsKey("pool")) {
      pool = "'" + context.getLocalProperties().get("pool") + "'";
    }
    String setPoolStmt = "if 'sc' in locals():\n\tsc.setLocalProperty('spark.scheduler.pool', " + pool + ")";
    callPython(new PythonInterpretRequest(setPoolStmt, false, false));
  }

  // Run python shell
  // Choose python in the order of
  // spark.pyspark.driver.python > spark.pyspark.python > PYSPARK_DRIVER_PYTHON > PYSPARK_PYTHON
  @Override
  protected String getPythonExec() {
    if (!StringUtils.isBlank(getProperty("spark.pyspark.driver.python", ""))) {
      return properties.getProperty("spark.pyspark.driver.python");
    }
    if (!StringUtils.isBlank(getProperty("spark.pyspark.python", ""))) {
      return properties.getProperty("spark.pyspark.python");
    }
    if (System.getenv("PYSPARK_PYTHON") != null) {
      return System.getenv("PYSPARK_PYTHON");
    }
    if (System.getenv("PYSPARK_DRIVER_PYTHON") != null) {
      return System.getenv("PYSPARK_DRIVER_PYTHON");
    }
    return "python";
  }

  public ZeppelinContext getZeppelinContext() {
    if (sparkInterpreter != null) {
      return sparkInterpreter.getZeppelinContext();
    } else {
      return null;
    }
  }

  /**
   * TODO 通过在zeppelin_pyspark.py里面使用intp.getJavaSparkContext()调用
   * @return
   */
  public JavaSparkContext getJavaSparkContext() {
    if (sparkInterpreter == null) {
      return null;
    } else {
      return new JavaSparkContext(sparkInterpreter.getSparkContext());
    }
  }

  public Object getSparkSession() {
    if (sparkInterpreter == null) {
      return null;
    } else {
      return sparkInterpreter.getSparkSession();
    }
  }

  /**
   * TODO 通过在zeppelin_pyspark.py里面使用intp.getSparkConf()调用
   * @return
   */
  public SparkConf getSparkConf() {
    JavaSparkContext sc = getJavaSparkContext();
    if (sc == null) {
      return null;
    } else {
      return sc.getConf();
    }
  }

  public Object getSQLContext() {
    if (sparkInterpreter == null) {
      return null;
    } else {
      return sparkInterpreter.getSQLContext();
    }
  }

  /**
   * TODO 通过在zeppelin_pyspark.py里面使用intp.isSpark1()调用
   * @return
   */
  public boolean isSpark1() {
    return sparkInterpreter.getSparkVersion().getMajorVersion() == 1;
  }

  /**
   * TODO 通过在zeppelin_pyspark.py里面使用intp.isSpark3()调用
   * @return
   */
  public boolean isSpark3() {
    return sparkInterpreter.getSparkVersion().getMajorVersion() == 3;
  }
}
