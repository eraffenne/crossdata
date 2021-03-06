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

package com.stratio.crossdata.server.mocks

import akka.actor.{Actor, Props}
import com.stratio.crossdata.common.result.{Result, QueryStatus}
import com.stratio.crossdata.communication.ACK
import com.stratio.crossdata.core.query.{StoragePlannedQuery, MetadataPlannedQuery, SelectPlannedQuery}
import org.apache.log4j.Logger

object MockCoordinatorActor{
  def props(): Props = Props(new MockCoordinatorActor())
}

/**
 * Actor in charge of the validation of sentences.
 */
class MockCoordinatorActor() extends Actor {
  lazy val logger = Logger.getLogger(classOf[MockCoordinatorActor])
  override def receive: Receive = {
    case query:MetadataPlannedQuery=>{
      logger.debug("MockCoordinator actor sending  EXECUTED")
      sender ! ACK(query.getQueryId,QueryStatus.EXECUTED)
    }
    case query:SelectPlannedQuery=>{
      logger.debug("MockCoordinator actor sending EXECUTED ")
      sender ! ACK(query.getQueryId,QueryStatus.EXECUTED)
    }
    case query:StoragePlannedQuery=>{
      logger.debug("MockCoordinator actor sending EXECUTED")
      sender ! ACK(query.getQueryId,QueryStatus.EXECUTED)
    }
    case _ => {
      logger.debug("Unknown message received by ValidatorActor");
      sender ! Result.createUnsupportedOperationErrorResult("Message not recognized")
    }
  }
}
