/*
 * Licensed to STRATIO (C) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership.  The STRATIO (C) licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.stratio.crossdata.server.actors

import akka.actor.{Actor, ActorRef, Props}
import com.stratio.crossdata.common.ask.{InsertBatchQuery, Query}
import com.stratio.crossdata.common.data.{TableName, CatalogName}
import com.stratio.crossdata.common.exceptions.{IgnoreQueryException, ValidationException, ParsingException}
import com.stratio.crossdata.common.result.Result
import com.stratio.crossdata.core.metadata.MetadataManagerException
import com.stratio.crossdata.core.parser.Parser
import com.stratio.crossdata.core.query.{StorageParsedQuery, IValidatedQuery, IParsedQuery, BaseQuery}
import com.stratio.crossdata.core.statements.InsertBatchStatement
import com.stratio.crossdata.core.validator.Validator
import org.apache.log4j.Logger
import scala.collection.JavaConversions._

object BusActor {
  def props(planner: ActorRef, validator: Validator): Props = Props(new BusActor(planner, validator))
}

/**
 * Actor in charge of validate batch inserts.
 * @param planner The associated planner actor.
 * @param validator The associated com.stratio.com.stratio.crossdata.core.validator.Validator}.
 */
class BusActor(planner: ActorRef, validator: Validator) extends Actor with TimeTracker {

  /**
   * Name of the timer published through JMX.
   */
  override lazy val timerName = this.getClass.getName
  /**
   * Class logger.
   */
  val log = Logger.getLogger(classOf[ValidatorActor])

  override def receive: Receive = {
    case query: InsertBatchQuery => {



      val timer = initTimer()
      var validatedQuery: IValidatedQuery = null
      try {

        val insertBatchStatement =  new InsertBatchStatement(query.fqTableName, query.columns)

        val tableName = new TableName(query.fqTableName.split("\\.")(0), query.fqTableName.split("\\.")(1))
        val validatedQuery = validator.validate(
           new StorageParsedQuery( new BaseQuery(query.queryId, "INSERT BATCH TO " + tableName,tableName.getCatalogName(),query.sessionId),insertBatchStatement)
        )

        //validatedQuery = validator.validate(query)
        log.info("Query validated")
        finishTimer(timer)
        planner forward validatedQuery

      } catch {
        case e: ValidationException => {
          log.info(e.getMessage())
          val result = Result.createValidationErrorResult(e)
          result.setQueryId(query.queryId)
          sender ! result
        }
        case e2: MetadataManagerException => {
          val result = Result.createExecutionErrorResult(e2.getMessage)
          result.setQueryId(query.queryId)
          sender ! result
        }
        case e3: IgnoreQueryException => {
          val result = Result.createExecutionErrorResult(e3.getMessage)
          result.setQueryId(query.queryId)
          sender ! result
        }
      }
    }
    case unknown: Any => {
      log.error(unknown)
      sender ! Result.createUnsupportedOperationErrorResult("Message not recognized")
    }
  }
}

