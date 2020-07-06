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
package org.apache.zeppelin.cassandra

import com.datastax.oss.driver.api.core.{ConsistencyLevel, DefaultConsistencyLevel}
import com.datastax.oss.driver.api.core.cql.{BatchType, DefaultBatchType}
import org.apache.zeppelin.interpreter.InterpreterException

import scala.util.matching.Regex
import scala.util.parsing.combinator._
import org.apache.zeppelin.cassandra.TextBlockHierarchy._

/**
  * Parser using Scala combinator parsing
  *
  * (?i) means case-insensitive mode
  * (?s) means DOT ALL mode
  * (?is) means case-insensitive and DOT ALL mode
  *
  */
object ParagraphParser {

  val allConsistencyLevels: Seq[ConsistencyLevel] = DefaultConsistencyLevel.values().toSeq

  val CONSISTENCY_LEVEL_PATTERN: Regex = allConsistencyLevels.filter(!_.isSerial).map(_.name)
    .mkString("""^\s*@consistency\s*=\s*(""", "|" , """)\s*$""").r

  val SERIAL_CONSISTENCY_LEVEL_PATTERN: Regex = allConsistencyLevels.filter(_.isSerial).map(_.name)
  .mkString("""^\s*@serialConsistency\s*=\s*(""", "|", """)\s*$""").r
  val TIMESTAMP_PATTERN: Regex = """^\s*@timestamp\s*=\s*([0-9]+)\s*$""".r

  val FETCHSIZE_PATTERN: Regex = """^\s*@fetchSize\s*=\s*([0-9]+)\s*$""".r
  val REQUEST_TIMEOUT_PATTERN: Regex = """^\s*@requestTimeOut\s*=\s*([0-9]+)\s*$""".r

  val SIMPLE_STATEMENT_PATTERN: Regex = """([^;]+;)""".r
  val PREPARE_STATEMENT_PATTERN: Regex = """^\s*@prepare\[([^]]+)\]\s*=\s*([^;]+)$""".r
  val REMOVE_PREPARE_STATEMENT_PATTERN: Regex = """^\s*@remove_prepare\[([^]]+)\]\s*$""".r

  val BIND_PATTERN: Regex = """^\s*@bind\[([^]]+)\](?:=([^;]+))?""".r
  val BATCH_PATTERN: Regex = """^(?i)\s*BEGIN\s+(UNLOGGED|COUNTER)?\s*BATCH""".r

  /**
    * Very complicated RegExp
    * (?: OR REPLACE)? -> optional presence of OR REPLACE
    * .+?  -> match ANY character in RELUCTANT mode
    * (?:\s*|\n|\r|\f) -> white space OR line returns (\n, \r, \f)
    * (?:\s*|\n|\r|\f)AS(?:\s*|\n|\r|\f) -> AS preceded and followed by white space or line return
    * (?:'|\$\$) -> simple quote (') OR double dollar ($$) as source code separator
    * (?:'|\$\$).+?(?:'|\$\$)\s*; ->
    *                                source code separator (?:'|\$\$)
    *                                followed by ANY character in RELUCTANT mode (.+?)
    *                                followed by source code separator (?:'|\$\$)
    *                                followed by optional white-space(s) (\s*)
    *                                followed by semi-colon (;)
    */
  val UDF_PATTERN: Regex = """(?is)\s*(CREATE(?:\s+OR REPLACE)?\s+FUNCTION(?:\s+IF\s+NOT\s+EXISTS)?.+?(?:\s+|\n|\r|\f)AS(?:\s+|\n|\r|\f)(?:'|\$\$).+?(?:'|\$\$)\s*;)""".r

  val GENERIC_STATEMENT_PREFIX: Regex =
    """(?is)\s*(?:INSERT|UPDATE|DELETE|SELECT|CREATE|ALTER|
      |DROP|GRANT|REVOKE|TRUNCATE|LIST|USE)\s+""".r

  val VALID_IDENTIFIER = "[a-z][a-z0-9_]*"

  val DESCRIBE_CLUSTER_PATTERN: Regex = """^(?i)\s*(?:DESCRIBE|DESC)\s+CLUSTER\s*;\s*$""".r


  val DESCRIBE_KEYSPACE_PATTERN: Regex = ("""^(?i)\s*(?:DESCRIBE|DESC)\s+KEYSPACE\s*("""+VALID_IDENTIFIER+""");\s*$""").r
  val DESCRIBE_KEYSPACES_PATTERN: Regex = """^(?i)\s*(?:DESCRIBE|DESC)\s+KEYSPACES\s*;\s*$""".r


  val DESCRIBE_TABLE_PATTERN: Regex = ("""^(?i)\s*(?:DESCRIBE|DESC)\s+TABLE\s*("""+VALID_IDENTIFIER+""");\s*$""").r
  val DESCRIBE_TABLE_WITH_KEYSPACE_PATTERN: Regex = ("""^(?i)\s*(?:DESCRIBE|DESC)\s+TABLE\s*(""" +
                                                VALID_IDENTIFIER +
                                                """)\.(""" +
                                                VALID_IDENTIFIER +
                                                """);\s*$""").r
  val DESCRIBE_TABLES_PATTERN: Regex = """^(?i)\s*(?:DESCRIBE|DESC)\s+TABLES\s*;\s*$""".r


  val DESCRIBE_TYPE_PATTERN: Regex = ("""^(?i)\s*(?:DESCRIBE|DESC)\s+TYPE\s*("""+VALID_IDENTIFIER+""");\s*$""").r
  val DESCRIBE_TYPE_WITH_KEYSPACE_PATTERN: Regex = ("""^(?i)\s*(?:DESCRIBE|DESC)\s+TYPE\s*(""" +
                                                VALID_IDENTIFIER +
                                                """)\.(""" +
                                                VALID_IDENTIFIER +
                                                """);\s*$""").r
  val DESCRIBE_TYPES_PATTERN: Regex = """^(?i)\s*(?:DESCRIBE|DESC)\s+TYPES\s*;\s*$""".r


  val DESCRIBE_FUNCTION_PATTERN: Regex = ("""^(?i)\s*(?:DESCRIBE|DESC)\s+FUNCTION\s*("""+VALID_IDENTIFIER+""");\s*$""").r
  val DESCRIBE_FUNCTION_WITH_KEYSPACE_PATTERN: Regex = ("""^(?i)\s*(?:DESCRIBE|DESC)\s+FUNCTION\s*(""" +
                                                  VALID_IDENTIFIER +
                                                  """)\.(""" +
                                                  VALID_IDENTIFIER +
                                                  """);\s*$""").r
  val DESCRIBE_FUNCTIONS_PATTERN: Regex = """^(?i)\s*(?:DESCRIBE|DESC)\s+FUNCTIONS\s*;\s*$""".r


  val DESCRIBE_AGGREGATE_PATTERN: Regex = ("""^(?i)\s*(?:DESCRIBE|DESC)\s+AGGREGATE\s*("""+VALID_IDENTIFIER+""");\s*$""").r
  val DESCRIBE_AGGREGATE_WITH_KEYSPACE_PATTERN: Regex = ("""^(?i)\s*(?:DESCRIBE|DESC)\s+AGGREGATE\s*(""" +
                                                    VALID_IDENTIFIER +
                                                    """)\.(""" +
                                                    VALID_IDENTIFIER +
                                                    """);\s*$""").r
  val DESCRIBE_AGGREGATES_PATTERN: Regex = """^(?i)\s*(?:DESCRIBE|DESC)\s+AGGREGATES\s*;\s*$""".r


  val DESCRIBE_MATERIALIZED_VIEW_PATTERN: Regex = ("""^(?i)\s*(?:DESCRIBE|DESC)\s+MATERIALIZED\s+VIEW\s*("""+VALID_IDENTIFIER+""")\s*;\s*$""").r
  val DESCRIBE_MATERIALIZED_VIEW_WITH_KEYSPACE_PATTERN: Regex = ("""^(?i)\s*(?:DESCRIBE|DESC)\s+MATERIALIZED\s+VIEW\s*(""" +
                                                            VALID_IDENTIFIER +
                                                            """)\.(""" +
                                                            VALID_IDENTIFIER +
                                                            """)\s*;\s*$""").r
  val DESCRIBE_MATERIALIZED_VIEWS_PATTERN: Regex = """^(?i)\s*(?:DESCRIBE|DESC)\s+MATERIALIZED\s+VIEWS\s*;\s*$""".r

  val HELP_PATTERN: Regex = """^(?i)\s*HELP;\s*$""".r
}

class ParagraphParser extends RegexParsers{

  import ParagraphParser._

  def singleLineCommentHash: Parser[Comment] = """\s*#.*""".r ^^ {text => Comment(text.trim.replaceAll("#",""))}
  def singleLineCommentDoubleSlashes: Parser[Comment] = """\s*//.*""".r ^^ {text => Comment(text.trim.replaceFirst("//\\s*",""))}
  def singleLineCommentDoubleDash: Parser[Comment] = """\s*--.*""".r ^^ {text => Comment(text.trim.replaceFirst("//\\s*",""))}
  def singleLineComment: Parser[Comment] = singleLineCommentHash | singleLineCommentDoubleSlashes | singleLineCommentDoubleDash

  def multiLineComment: Parser[Comment] = """(?s)/\*(.*)\*/""".r ^^ {text => Comment(text.trim.replaceAll("""/\*""","").replaceAll("""\*/""",""))}

  //Query parameters
  def consistency: Parser[Consistency] = """\s*@consistency.+""".r ^^ {x => extractConsistency(x.trim)}
  def serialConsistency: Parser[SerialConsistency] = """\s*@serialConsistency.+""".r ^^ {x => extractSerialConsistency(x.trim)}
  def timestamp: Parser[Timestamp] = """\s*@timestamp[^F].+""".r ^^ {x => extractTimestamp(x.trim)}
  def fetchSize: Parser[FetchSize] = """\s*@fetchSize.+""".r ^^ {x => extractFetchSize(x.trim)}
  def requestTimeOut: Parser[RequestTimeOut] = """\s*@requestTimeOut.+""".r ^^ {x => extractRequestTimeOut(x.trim)}

  //Statements
  def createFunctionStatement(): Parser[SimpleStm] = UDF_PATTERN ^^{x => extractUdfStatement(x.trim)}
  def genericStatement(): Parser[SimpleStm] = s"""$GENERIC_STATEMENT_PREFIX[^;]+;""".r ^^ {x => extractSimpleStatement(x.trim)}
//  def allStatement: Parser[SimpleStm] = udfStatement | genericStatement

  def prepare(): Parser[PrepareStm] = """\s*@prepare.+""".r ^^ {x => extractPreparedStatement(x.trim)}
  def removePrepare(): Parser[RemovePrepareStm] = """\s*@remove_prepare.+""".r ^^ {x => extractRemovePreparedStatement(x.trim)}
  def bind(): Parser[BoundStm] = """\s*@bind.+""".r ^^ {x => extractBoundStatement(x.trim)}

  //Meta data
  private def describeCluster: Parser[DescribeClusterCmd] = """(?i)\s*(?:DESCRIBE|DESC)\s+CLUSTER.*""".r ^^ extractDescribeClusterCmd
  private def describeKeyspaces: Parser[DescribeKeyspacesCmd] = """(?i)\s*(?:DESCRIBE|DESC)\s+KEYSPACES.*""".r ^^ extractDescribeKeyspacesCmd
  private def describeTables: Parser[DescribeTablesCmd] = """(?i)\s*(?:DESCRIBE|DESC)\s+TABLES.*""".r ^^ extractDescribeTablesCmd
  private def describeTypes: Parser[DescribeTypesCmd] = """(?i)\s*(?:DESCRIBE|DESC)\s+TYPES.*""".r ^^ extractDescribeTypesCmd
  private def describeFunctions: Parser[DescribeFunctionsCmd] = """(?i)\s*(?:DESCRIBE|DESC)\s+FUNCTIONS.*""".r ^^ extractDescribeFunctionsCmd
  private def describeAggregates: Parser[DescribeAggregatesCmd] = """(?i)\s*(?:DESCRIBE|DESC)\s+AGGREGATES.*""".r ^^ extractDescribeAggregatesCmd
  private def describeMaterializedViews: Parser[DescribeMaterializedViewsCmd] = """(?i)\s*(?:DESCRIBE|DESC)\s+MATERIALIZED\s+VIEWS.*""".r ^^ extractDescribeMaterializedViewsCmd
  private def describeKeyspace: Parser[DescribeKeyspaceCmd] = """\s*(?i)(?:DESCRIBE|DESC)\s+KEYSPACE\s+.+""".r ^^ extractDescribeKeyspaceCmd
  private def describeTable: Parser[DescribeTableCmd] = """(?i)\s*(?:DESCRIBE|DESC)\s+TABLE\s+.+""".r ^^ extractDescribeTableCmd
  private def describeType: Parser[DescribeTypeCmd] = """(?i)\s*(?:DESCRIBE|DESC)\s+TYPE\s+.*""".r ^^ extractDescribeTypeCmd
  private def describeFunction: Parser[DescribeFunctionCmd] = """(?i)\s*(?:DESCRIBE|DESC)\s+FUNCTION\s+.*""".r ^^ extractDescribeFunctionCmd
  private def describeAggregate: Parser[DescribeAggregateCmd] = """(?i)\s*(?:DESCRIBE|DESC)\s+AGGREGATE\s+.*""".r ^^ extractDescribeAggregateCmd
  private def describeMaterializedView: Parser[DescribeMaterializedViewCmd] = """(?i)\s*(?:DESCRIBE|DESC)\s+MATERIALIZED\s+VIEW\s+.*""".r ^^ extractDescribeMaterializedViewCmd


  //Help
  private def helpCommand: Parser[HelpCmd] = """(?i)\s*HELP.*""".r ^^ extractHelpCmd

  private def beginBatch: Parser[String] = """(?i)\s*BEGIN\s+(UNLOGGED|COUNTER)?\s*BATCH""".r
  private def applyBatch: Parser[String] = """(?i)APPLY BATCH;""".r
  private def insert: Parser[SimpleStm] = """(?i)INSERT [^;]+;""".r ^^ SimpleStm
  private def update: Parser[SimpleStm] = """(?i)UPDATE [^;]+;""".r ^^ SimpleStm
  private def delete: Parser[SimpleStm] = """(?i)DELETE [^;]+;""".r ^^ SimpleStm

  private def mutationStatements: Parser[List[QueryStatement]] = rep(insert | update | delete | bind)

  def batch: Parser[BatchStm] = beginBatch ~ mutationStatements ~ applyBatch ^^ {
    case begin ~ cqls ~ end => BatchStm(extractBatchType(begin), cqls)
  }

  def queries:Parser[List[AnyBlock]] = rep(singleLineComment | multiLineComment | consistency | serialConsistency |
    timestamp | fetchSize | requestTimeOut | removePrepare | prepare | bind | batch |
    describeKeyspace | describeKeyspaces |
    describeTable | describeTables |
    describeType | describeTypes |
    describeFunction | describeFunctions |
    describeAggregate | describeAggregates |
    describeMaterializedView | describeMaterializedViews |
    describeCluster | helpCommand | createFunctionStatement | genericStatement)

  def extractConsistency(text: String): Consistency = {
    text match {
      case CONSISTENCY_LEVEL_PATTERN(consistency) => Consistency(DefaultConsistencyLevel.valueOf(consistency))
      case _ => throw new InterpreterException(s"Invalid syntax for @consistency. " +
        s"It should comply to the pattern ${CONSISTENCY_LEVEL_PATTERN.toString}")
    }
  }

  def extractSerialConsistency(text: String): SerialConsistency = {
    text match {
      case SERIAL_CONSISTENCY_LEVEL_PATTERN(consistency) =>
        SerialConsistency(DefaultConsistencyLevel.valueOf(consistency))
      case _ => throw new InterpreterException(s"Invalid syntax for @serialConsistency. " +
        s"It should comply to the pattern ${SERIAL_CONSISTENCY_LEVEL_PATTERN.toString}")
    }
  }

  def extractTimestamp(text: String): Timestamp = {
    text match {
      case TIMESTAMP_PATTERN(timestamp) => Timestamp(timestamp.trim.toLong)
      case _ => throw new InterpreterException(s"Invalid syntax for @timestamp. " +
        s"It should comply to the pattern ${TIMESTAMP_PATTERN.toString}")
    }
  }

  def extractFetchSize(text: String): FetchSize = {
    text match {
      case FETCHSIZE_PATTERN(fetchSize) =>
        FetchSize(fetchSize.trim.toInt)
      case _ => throw new InterpreterException(s"Invalid syntax for @fetchSize. " +
        s"It should comply to the pattern ${FETCHSIZE_PATTERN.toString}")
    }
  }

  def extractRequestTimeOut(text: String): RequestTimeOut = {
    text match {
      case REQUEST_TIMEOUT_PATTERN(requestTimeOut) =>
        RequestTimeOut(requestTimeOut.trim.toInt)
      case _ => throw new InterpreterException(s"Invalid syntax for @requestTimeOut. " +
        s"It should comply to the pattern ${REQUEST_TIMEOUT_PATTERN.toString}")
    }
  }

  def extractSimpleStatement(text: String): SimpleStm = {
    text match {
      case SIMPLE_STATEMENT_PATTERN(statement) =>
        SimpleStm(statement)
      case _ => throw new InterpreterException(s"Invalid statement '$text'. Did you forget to add ; (semi-colon) at the end of each CQL statement ?")
    }
  }

  def extractUdfStatement(text: String): SimpleStm = {
    text match {
      case UDF_PATTERN(statement) =>
        SimpleStm(statement)
      case _ => throw new InterpreterException(s"Invalid statement '$text' for UDF creation. Did you forget to add ; (semi-colon) at the end of each CQL statement ?")
    }
  }

  def extractPreparedStatement(text: String): PrepareStm = {
    text match {
      case PREPARE_STATEMENT_PATTERN(name,queryString) =>
        PrepareStm(name.trim,queryString.trim)
      case _ => throw new InterpreterException(s"Invalid syntax for @prepare. " +
        s"It should comply to the pattern: @prepare[prepared_statement_name]=CQL Statement (without semi-colon)")
    }
  }

  def extractRemovePreparedStatement(text: String): RemovePrepareStm= {
    text match {
      case REMOVE_PREPARE_STATEMENT_PATTERN(name) =>
        RemovePrepareStm(name.trim)
      case _ => throw new InterpreterException(s"Invalid syntax for @remove_prepare. " +
        s"It should comply to the pattern: @remove_prepare[prepared_statement_name]")
    }
  }

  def extractBoundStatement(text: String): BoundStm = {
    text match {
      case BIND_PATTERN(name,boundValues) =>
        BoundStm(name.trim, Option(boundValues).map(_.trim).getOrElse(""))
      case _ => throw new InterpreterException("Invalid syntax for @bind. It should comply to the pattern: " +
        "@bind[prepared_statement_name]=10,'jdoe','John DOE',12345,'2015-07-32 12:04:23.234' " +
        "OR @bind[prepared_statement_name] with no bound value. No semi-colon")
    }
  }

  def extractBatchType(text: String): BatchType = {
    text match {
      case BATCH_PATTERN(batchType) =>
        val inferredType = Option(batchType).getOrElse("LOGGED")
        DefaultBatchType.valueOf(inferredType.toUpperCase)
      case _ => throw new InterpreterException(s"Invalid syntax for BEGIN BATCH. " +
        s"""It should comply to the pattern: ${BATCH_PATTERN.toString}""")
    }
  }

  def extractDescribeClusterCmd(text: String): DescribeClusterCmd = {
    text match {
      case DESCRIBE_CLUSTER_PATTERN() =>
        new DescribeClusterCmd
      case _ => throw new InterpreterException(s"Invalid syntax for DESCRIBE CLUSTER. " +
        s"""It should comply to the pattern: ${DESCRIBE_CLUSTER_PATTERN.toString}""")
    }
  }

  def extractDescribeKeyspaceCmd(text: String): DescribeKeyspaceCmd = {
    text match {
      case DESCRIBE_KEYSPACE_PATTERN(keyspace) =>
        DescribeKeyspaceCmd(keyspace)
      case _ => throw new InterpreterException(s"Invalid syntax for DESCRIBE KEYSPACE. " +
        s"""It should comply to the pattern: ${DESCRIBE_KEYSPACE_PATTERN.toString}""")
    }
  }

  def extractDescribeKeyspacesCmd(text: String): DescribeKeyspacesCmd = {
    text match {
        case DESCRIBE_KEYSPACES_PATTERN() =>
          new DescribeKeyspacesCmd
        case _ => throw new InterpreterException(s"Invalid syntax for DESCRIBE KEYSPACES. " +
          s"""It should comply to the pattern: ${DESCRIBE_KEYSPACES_PATTERN.toString}""")
      }
  }

  def extractDescribeTableCmd(text: String): DescribeTableCmd = {
    text match {
      case DESCRIBE_TABLE_WITH_KEYSPACE_PATTERN(keyspace,table) =>
        DescribeTableCmd(Option(keyspace),table)
      case DESCRIBE_TABLE_PATTERN(table) =>
        DescribeTableCmd(Option.empty,table)
      case _ => throw new InterpreterException(s"Invalid syntax for DESCRIBE TABLE. " +
        s"""It should comply to the patterns: ${DESCRIBE_TABLE_WITH_KEYSPACE_PATTERN.toString} or ${DESCRIBE_TABLE_PATTERN.toString}""".stripMargin)
    }
  }

  def extractDescribeTablesCmd(text: String): DescribeTablesCmd = {
    text match {
      case DESCRIBE_TABLES_PATTERN() =>
        new DescribeTablesCmd
      case _ => throw new InterpreterException(s"Invalid syntax for DESCRIBE TABLES. " +
        s"""It should comply to the pattern: ${DESCRIBE_TABLES_PATTERN.toString}""")
    }
  }

  def extractDescribeTypeCmd(text: String): DescribeTypeCmd = {
    text match {
      case DESCRIBE_TYPE_WITH_KEYSPACE_PATTERN(keyspace,table) =>
        DescribeTypeCmd(Option(keyspace),table)
      case DESCRIBE_TYPE_PATTERN(table) =>
        DescribeTypeCmd(Option.empty,table)
      case _ => throw new InterpreterException(s"Invalid syntax for DESCRIBE TYPE. " +
        s"""It should comply to the patterns: ${DESCRIBE_TYPE_WITH_KEYSPACE_PATTERN.toString} or ${DESCRIBE_TYPE_PATTERN.toString}""".stripMargin)
    }
  }

  def extractDescribeTypesCmd(text: String): DescribeTypesCmd = {
    text match {
      case DESCRIBE_TYPES_PATTERN() =>
        new DescribeTypesCmd
      case _ => throw new InterpreterException(s"Invalid syntax for DESCRIBE TYPES. " +
        s"""It should comply to the pattern: ${DESCRIBE_TYPES_PATTERN.toString}""")
    }
  }

  def extractDescribeFunctionCmd(text: String): DescribeFunctionCmd = {
    text match {
      case DESCRIBE_FUNCTION_WITH_KEYSPACE_PATTERN(keyspace,function) =>
        DescribeFunctionCmd(Option(keyspace),function)
      case DESCRIBE_FUNCTION_PATTERN(function) =>
        DescribeFunctionCmd(Option.empty,function)
      case _ => throw new InterpreterException(s"Invalid syntax for DESCRIBE FUNCTION. " +
        s"""It should comply to the patterns: ${DESCRIBE_FUNCTION_WITH_KEYSPACE_PATTERN.toString} or ${DESCRIBE_FUNCTION_PATTERN.toString}""".stripMargin)
    }
  }

  def extractDescribeFunctionsCmd(text: String): DescribeFunctionsCmd = {
    text match {
      case DESCRIBE_FUNCTIONS_PATTERN() =>
        new DescribeFunctionsCmd
      case _ => throw new InterpreterException(s"Invalid syntax for DESCRIBE FUNCTIONS. " +
        s"""It should comply to the pattern: ${DESCRIBE_FUNCTIONS_PATTERN.toString}""".stripMargin)
    }
  }

  def extractDescribeAggregateCmd(text: String): DescribeAggregateCmd = {
    text match {
      case DESCRIBE_AGGREGATE_WITH_KEYSPACE_PATTERN(keyspace,aggregate) =>
        DescribeAggregateCmd(Option(keyspace),aggregate)
      case DESCRIBE_AGGREGATE_PATTERN(aggregate) =>
        DescribeAggregateCmd(Option.empty,aggregate)
      case _ => throw new InterpreterException(s"Invalid syntax for DESCRIBE AGGREGATE. " +
        s"""It should comply to the patterns: ${DESCRIBE_AGGREGATE_WITH_KEYSPACE_PATTERN.toString} or ${DESCRIBE_AGGREGATE_PATTERN.toString}""".stripMargin)
    }
  }

  def extractDescribeAggregatesCmd(text: String): DescribeAggregatesCmd = {
    text match {
      case DESCRIBE_AGGREGATES_PATTERN() =>
        new DescribeAggregatesCmd
      case _ => throw new InterpreterException(s"Invalid syntax for DESCRIBE AGGREGATES. " +
        s"""It should comply to the pattern: ${DESCRIBE_AGGREGATES_PATTERN.toString}""".stripMargin)
    }
  }

  def extractDescribeMaterializedViewCmd(text: String): DescribeMaterializedViewCmd = {
    text match {
      case DESCRIBE_MATERIALIZED_VIEW_WITH_KEYSPACE_PATTERN(keyspace,view) =>
        DescribeMaterializedViewCmd(Option(keyspace),view)
      case DESCRIBE_MATERIALIZED_VIEW_PATTERN(view) =>
        DescribeMaterializedViewCmd(Option.empty,view)
      case _ => throw new InterpreterException(s"Invalid syntax for DESCRIBE MATERIALIZED VIEW. " +
        s"""It should comply to the patterns: ${DESCRIBE_MATERIALIZED_VIEW_WITH_KEYSPACE_PATTERN.toString} or ${DESCRIBE_MATERIALIZED_VIEW_PATTERN.toString}""".stripMargin)
    }
  }

  def extractDescribeMaterializedViewsCmd(text: String): DescribeMaterializedViewsCmd = {
    text match {
      case DESCRIBE_MATERIALIZED_VIEWS_PATTERN() =>
        new DescribeMaterializedViewsCmd
      case _ => throw new InterpreterException(s"Invalid syntax for DESCRIBE MATERIALIZED VIEWS. " +
        s"""It should comply to the pattern: ${DESCRIBE_MATERIALIZED_VIEWS_PATTERN.toString}""".stripMargin)
    }
  }

  def extractHelpCmd(text: String): HelpCmd = {
    text match {
      case HELP_PATTERN() =>
        new HelpCmd
      case _ => throw new InterpreterException(s"Invalid syntax for HELP. " +
        s"""It should comply to the patterns: ${HELP_PATTERN.toString}""".stripMargin)
    }
  }
}

