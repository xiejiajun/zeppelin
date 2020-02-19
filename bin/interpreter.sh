#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


bin=$(dirname "${BASH_SOURCE-$0}")
bin=$(cd "${bin}">/dev/null; pwd)

function usage() {
    echo "usage) $0 -p <port> -r <intp_port> -d <interpreter dir to load> -l <local interpreter repo dir to load> -g <interpreter group name>"
}
## 用于标记是哪个用户启动的解释器，不做其他特殊用途
INTERPRETER_USER_TAG=""
while getopts "hc:p:r:d:l:v:u:g:t:" o; do
    case ${o} in
        h)
            usage
            exit 0
            ;;
        d)
            INTERPRETER_DIR=${OPTARG}
            ;;
        c)
            CALLBACK_HOST=${OPTARG} # This will be used callback host
            ;;
        p)
            PORT=${OPTARG} # This will be used for callback port
            ;;
        r)
            INTP_PORT=${OPTARG} # This will be used for interpreter process port
            ;;
        l)
            LOCAL_INTERPRETER_REPO=${OPTARG}
            ;;
        v)
            . "${bin}/common.sh"
            getZeppelinVersion
            ;;
        u)
            ZEPPELIN_IMPERSONATE_USER="${OPTARG}"
            ;;
        g)
            INTERPRETER_SETTING_NAME=${OPTARG}
            ;;
        t)
            INTERPRETER_USER_TAG=${OPTARG}
            ;;
        esac
done


if [ -z "${PORT}" ] || [ -z "${INTERPRETER_DIR}" ]; then
    usage
    exit 1
fi

. "${bin}/common.sh"

ZEPPELIN_INTP_CLASSPATH="${CLASSPATH}"

# construct classpath
if [[ -d "${ZEPPELIN_HOME}/zeppelin-interpreter/target/classes" ]]; then
  ZEPPELIN_INTP_CLASSPATH+=":${ZEPPELIN_HOME}/zeppelin-interpreter/target/classes"
fi

# add test classes for unittest
if [[ -d "${ZEPPELIN_HOME}/zeppelin-interpreter/target/test-classes" ]]; then
  ZEPPELIN_INTP_CLASSPATH+=":${ZEPPELIN_HOME}/zeppelin-interpreter/target/test-classes"
fi
if [[ -d "${ZEPPELIN_HOME}/zeppelin-zengine/target/test-classes" ]]; then
  ZEPPELIN_INTP_CLASSPATH+=":${ZEPPELIN_HOME}/zeppelin-zengine/target/test-classes"
fi

addJarInDirForIntp "${ZEPPELIN_HOME}/zeppelin-interpreter/target/lib"
addJarInDirForIntp "${ZEPPELIN_HOME}/lib/interpreter"
addJarInDirForIntp "${INTERPRETER_DIR}"

HOSTNAME=$(hostname)
ZEPPELIN_SERVER=org.apache.zeppelin.interpreter.remote.RemoteInterpreterServer

INTERPRETER_ID=$(basename "${INTERPRETER_DIR}")
ZEPPELIN_PID="${ZEPPELIN_PID_DIR}/zeppelin-interpreter-${INTERPRETER_ID}-${ZEPPELIN_IDENT_STRING}-${HOSTNAME}.pid"
ZEPPELIN_LOGFILE="${ZEPPELIN_LOG_DIR}/zeppelin-interpreter-${INTERPRETER_SETTING_NAME}-"

if [[ -z "$ZEPPELIN_IMPERSONATE_CMD" ]]; then
    if [[ "${INTERPRETER_ID}" != "spark" || "$ZEPPELIN_IMPERSONATE_SPARK_PROXY_USER" == "false" ]]; then
        ZEPPELIN_IMPERSONATE_RUN_CMD=`echo "ssh ${ZEPPELIN_IMPERSONATE_USER}@localhost" `
    fi
else
    ZEPPELIN_IMPERSONATE_RUN_CMD=$(eval "echo ${ZEPPELIN_IMPERSONATE_CMD} ")
fi


if [[ ! -z "$ZEPPELIN_IMPERSONATE_USER" ]]; then
    ZEPPELIN_LOGFILE+="${ZEPPELIN_IMPERSONATE_USER}-"
fi

if [[ -z "$ZEPPELIN_IMPERSONATE_USER"  && -n "${INTERPRETER_USER_TAG}" ]]; then
   ZEPPELIN_LOGFILE+="${INTERPRETER_USER_TAG}-"
fi

ZEPPELIN_LOGFILE+="${ZEPPELIN_IDENT_STRING}-${HOSTNAME}.log"
JAVA_INTP_OPTS+=" -Dzeppelin.log.file=${ZEPPELIN_LOGFILE}"

if [[ ! -d "${ZEPPELIN_LOG_DIR}" ]]; then
  echo "Log dir doesn't exist, create ${ZEPPELIN_LOG_DIR}"
  $(mkdir -p "${ZEPPELIN_LOG_DIR}")
fi

# set spark related env variables
if [[ "${INTERPRETER_ID}" == "spark" ]]; then

  # run kinit
  if [[ -n "${ZEPPELIN_SERVER_KERBEROS_KEYTAB}" ]] && [[ -n "${ZEPPELIN_SERVER_KERBEROS_PRINCIPAL}" ]]; then
    kinit -kt ${ZEPPELIN_SERVER_KERBEROS_KEYTAB} ${ZEPPELIN_SERVER_KERBEROS_PRINCIPAL}
  fi
  if [[ -n "${SPARK_HOME}" ]]; then
    export SPARK_SUBMIT="${SPARK_HOME}/bin/spark-submit"
    SPARK_APP_JAR="$(ls ${ZEPPELIN_HOME}/interpreter/spark/spark-interpreter*.jar)"
    # This will evantually passes SPARK_APP_JAR to classpath of SparkIMain
    ZEPPELIN_INTP_CLASSPATH+=":${SPARK_APP_JAR}"

    pattern="$SPARK_HOME/python/lib/py4j-*-src.zip"
    py4j=($pattern)
    # pick the first match py4j zip - there should only be one
    export PYTHONPATH="$SPARK_HOME/python/:$PYTHONPATH"
    export PYTHONPATH="${py4j[0]}:$PYTHONPATH"
  else
    # add Hadoop jars into classpath
    if [[ -n "${HADOOP_HOME}" ]]; then
      # Apache
      addEachJarInDirRecursiveForIntp "${HADOOP_HOME}/share"

      # CDH
      addJarInDirForIntp "${HADOOP_HOME}"
      addJarInDirForIntp "${HADOOP_HOME}/lib"
    fi

    addJarInDirForIntp "${INTERPRETER_DIR}/dep"

    pattern="${ZEPPELIN_HOME}/interpreter/spark/pyspark/py4j-*-src.zip"
    py4j=($pattern)
    # pick the first match py4j zip - there should only be one
    PYSPARKPATH="${ZEPPELIN_HOME}/interpreter/spark/pyspark/pyspark.zip:${py4j[0]}"

    if [[ -z "${PYTHONPATH}" ]]; then
      export PYTHONPATH="${PYSPARKPATH}"
    else
      export PYTHONPATH="${PYTHONPATH}:${PYSPARKPATH}"
    fi
    unset PYSPARKPATH
    export SPARK_CLASSPATH+=":${ZEPPELIN_INTP_CLASSPATH}"
  fi

  if [[ -n "${HADOOP_CONF_DIR}" ]] && [[ -d "${HADOOP_CONF_DIR}" ]]; then
    ZEPPELIN_INTP_CLASSPATH+=":${HADOOP_CONF_DIR}"
    export HADOOP_CONF_DIR=${HADOOP_CONF_DIR}
  else
    # autodetect HADOOP_CONF_HOME by heuristic
    if [[ -n "${HADOOP_HOME}" ]] && [[ -z "${HADOOP_CONF_DIR}" ]]; then
      if [[ -d "${HADOOP_HOME}/etc/hadoop" ]]; then
        export HADOOP_CONF_DIR="${HADOOP_HOME}/etc/hadoop"
      elif [[ -d "/etc/hadoop/conf" ]]; then
        export HADOOP_CONF_DIR="/etc/hadoop/conf"
      fi
    fi
  fi

elif [[ "${INTERPRETER_ID}" == "hbase" ]]; then
  if [[ -n "${HBASE_CONF_DIR}" ]]; then
    ZEPPELIN_INTP_CLASSPATH+=":${HBASE_CONF_DIR}"
  elif [[ -n "${HBASE_HOME}" ]]; then
    ZEPPELIN_INTP_CLASSPATH+=":${HBASE_HOME}/conf"
  else
    echo "HBASE_HOME and HBASE_CONF_DIR are not set, configuration might not be loaded"
  fi
elif [[ "${INTERPRETER_ID}" == "pig" ]]; then
   # autodetect HADOOP_CONF_HOME by heuristic
  if [[ -n "${HADOOP_HOME}" ]] && [[ -z "${HADOOP_CONF_DIR}" ]]; then
    if [[ -d "${HADOOP_HOME}/etc/hadoop" ]]; then
      export HADOOP_CONF_DIR="${HADOOP_HOME}/etc/hadoop"
    elif [[ -d "/etc/hadoop/conf" ]]; then
      export HADOOP_CONF_DIR="/etc/hadoop/conf"
    fi
  fi

  if [[ -n "${HADOOP_CONF_DIR}" ]] && [[ -d "${HADOOP_CONF_DIR}" ]]; then
    ZEPPELIN_INTP_CLASSPATH+=":${HADOOP_CONF_DIR}"
  fi

  # autodetect TEZ_CONF_DIR
  if [[ -n "${TEZ_CONF_DIR}" ]]; then
    ZEPPELIN_INTP_CLASSPATH+=":${TEZ_CONF_DIR}"
  elif [[ -d "/etc/tez/conf" ]]; then
    ZEPPELIN_INTP_CLASSPATH+=":/etc/tez/conf"
  else
    echo "TEZ_CONF_DIR is not set, configuration might not be loaded"
  fi
fi

addJarInDirForIntp "${LOCAL_INTERPRETER_REPO}"

if [[ ! -z "$ZEPPELIN_IMPERSONATE_USER" ]]; then
  if [[ "${INTERPRETER_ID}" != "spark" || "$ZEPPELIN_IMPERSONATE_SPARK_PROXY_USER" == "false" ]]; then
    suid="$(id -u ${ZEPPELIN_IMPERSONATE_USER})"
    if [[ -n  "${suid}" || -z "${SPARK_SUBMIT}" ]]; then
       INTERPRETER_RUN_COMMAND=${ZEPPELIN_IMPERSONATE_RUN_CMD}" '"
       if [[ -f "${ZEPPELIN_CONF_DIR}/zeppelin-env.sh" ]]; then
           INTERPRETER_RUN_COMMAND+=" source "${ZEPPELIN_CONF_DIR}'/zeppelin-env.sh;'
       fi
    fi
  fi
fi

if [[ -n "${SPARK_SUBMIT}" ]]; then
    ## Spark解释器启动逻辑，直接通过spark-submit命令将spark-interpreter-xxx.jar作为Spark应用提交，
    # spark-interpreter-xxx.jar内部和我们平常写的Spark代码有所不同，我们平时一般不会有在在自己写的Spark应用内部实现一个常驻服务去不断接受客户端请求,
    # 但这里是以org.apache.zeppelin.interpreter.remote.RemoteInterpreterServer为Spark的MainClass将整个Spark解释器jar包作为SparkApp提交，以达到在Spark
    # Driver端启动一个监听用户交互式执行请求（这个常驻服务是运行在driver上的，且这里只支持yarn-client模式，driver起在本地，保证RPC调用通畅，这个实现方式应该是
    # 参照spark-shell的实现方式进行改造的），然后不断出通过Driver提交并运行这些请求所包含的业务逻辑。（这个Spark解释器应该也是类似于SparkStreaming的一个服务，
    # sparkStreaming应该也是这么实现的，只不过SparkStreaming是自己不断去拉数据来驱动SparkCore作业，而我们这里是被动监听客户端请求来触发SparkCore作业）
    # 我们平常写的SPARK APP除了流计算是常驻的,其他的应用基本上都是像命令行程序一样执行完就退出的
    ## 所以：spark-shell的交互式查询请求监听服务是否和SparkStreaming数据监听服务实现方案差不多呢？ 等待对Spark源码进行分析来解答
    INTERPRETER_RUN_COMMAND+=' '` echo ${SPARK_SUBMIT} --class ${ZEPPELIN_SERVER} --driver-class-path \"${ZEPPELIN_INTP_CLASSPATH_OVERRIDES}:${ZEPPELIN_INTP_CLASSPATH}\" --driver-java-options \"${JAVA_INTP_OPTS}\" ${SPARK_SUBMIT_OPTIONS} ${ZEPPELIN_SPARK_CONF} ${SPARK_APP_JAR} ${CALLBACK_HOST} ${PORT} ${INTP_PORT}`
else
    ## 其他解释器直接将RemoteInterpreterServer作为入口来启动一个普通Java进程监听用户请求的常驻服务的
    INTERPRETER_RUN_COMMAND+=' '` echo ${ZEPPELIN_RUNNER} ${JAVA_INTP_OPTS} ${ZEPPELIN_INTP_MEM} -cp ${ZEPPELIN_INTP_CLASSPATH_OVERRIDES}:${ZEPPELIN_INTP_CLASSPATH} ${ZEPPELIN_SERVER} ${CALLBACK_HOST} ${PORT} ${INTP_PORT}`
fi


if [[ ! -z "$ZEPPELIN_IMPERSONATE_USER" ]] && [[ -n "${suid}" || -z "${SPARK_SUBMIT}" ]]; then
    INTERPRETER_RUN_COMMAND+="'"
fi

eval $INTERPRETER_RUN_COMMAND &
pid=$!

if [[ -z "${pid}" ]]; then
  exit 1;
else
  echo ${pid} > ${ZEPPELIN_PID}
fi

### 使用trap命令进行SIGTERM SIGINT SIGQUIT信号响应（默认Ctrl+C 只执行一次kill -2，不一定杀得掉解释器，所以这里修改了杀进程行为，会重试）
trap 'shutdown_hook;' SIGTERM SIGINT SIGQUIT
### 对于有些时候页面上重启解释器一致重启不成功，卡住不动的情况，可以考虑把shutdown_hook函数的实现逻辑直接改成简单粗暴的kill -9,即如下实现
#function shutdown_hook() {
#    $(kill -9 ${pid} > /dev/null 2> /dev/null)
#    rm -f "${ZEPPELIN_PID}"
#}

function shutdown_hook() {
  local count
  count=0
  while [[ "${count}" -lt 10 ]]; do
    $(kill ${pid} > /dev/null 2> /dev/null)
    if kill -0 ${pid} > /dev/null 2>&1; then
      sleep 3
      let "count+=1"
    else
      rm -f "${ZEPPELIN_PID}"
      break
    fi
  if [[ "${count}" == "5" ]]; then
    $(kill -9 ${pid} > /dev/null 2> /dev/null)
    rm -f "${ZEPPELIN_PID}"
  fi
  done
}

wait
