# Apache Zeppelin

**Documentation:** [User Guide](https://zeppelin.apache.org/docs/latest/index.html)<br/>
**Mailing Lists:** [User and Dev mailing list](https://zeppelin.apache.org/community.html)<br/>
**Continuous Integration:** [![Build Status](https://travis-ci.org/apache/zeppelin.svg?branch=master)](https://travis-ci.org/apache/zeppelin) <br/>
**Contributing:** [Contribution Guide](https://zeppelin.apache.org/contribution/contributions.html)<br/>
**Issue Tracker:** [Jira](https://issues.apache.org/jira/browse/ZEPPELIN)<br/>
**License:** [Apache 2.0](https://github.com/apache/zeppelin/blob/master/LICENSE)


**Zeppelin**, a web-based notebook that enables interactive data analytics. You can make beautiful data-driven, interactive and collaborative documents with SQL, Scala and more.

Core feature:
   * Web based notebook style editor.
   * Built-in Apache Spark support


To know more about Zeppelin, visit our web site [http://zeppelin.apache.org](https://zeppelin.apache.org)


## Getting Started

### Install binary package
Please go to [install](https://zeppelin.apache.org/docs/latest/install/install.html) to install Apache Zeppelin from binary package.

### Build from source
Please check [Build from source](https://zeppelin.apache.org/docs/latest/install/build.html) to build Zeppelin from source.




### 段落运行流程分析
1. NotebookServer.onMessage -> runParagraph -> persistAndExecuteSingleParagraph -> Note.run
2. Note.run -> Paragraph.execute -> Scheduler.submit -> Scheduler.run -> ExecutorService.execute(new JobRunner(scheduler, job))
3. JobRunner.run -> Job.run -> 1.(启动作业进度更新器JobProgressPoller.start) -> Paragraph.jobRun -> 1. getBindedInterpreter
   获取连接对应解释器的代理客户端(RemoteInterpreter) —> interpreter.interpret(RemoteInterpreter.interpret)
4. RemoteInterpreter.interpret -> getOrCreateInterpreterProcess -> ManagedInterpreterGroup.getOrCreateInterpreterProcess 
  -> interpreterProcess.callRemoteFunction -> RemoteInterpreterProcess.getClient().interpret向远程解释器服务发起请求
  
  
---
#### 顺便分析下 Paragraph.jobRun里面调用的getBindedInterpreter获取到的RemoteInterpreter代理客户端是怎么初始化的
- Paragraph.getBindedInterpreter -> InterpreterFactory.getInterpreter 
    1. InterpreterSetting.getDefaultInterpreter -> InterpreterSetting.getOrCreateSession  -> ...（跟下面的逻辑一样了）
        - 从InterpreterSetting.getOrCreateSession返回的结果中取第一个作为RemoteInterpreter客户端
       
    2. InterpreterSetting.getInterpreter -> InterpreterSetting.getOrCreateSession -> ManagedInterpreterGroup.getOrCreateSession -> 
       InterpreterSetting.createInterpreters -> new RemoteInterpreter()
       - 以className为key从InterpreterSetting.getOrCreateSession返回的结果中查找对应的RemoteInterpreter


