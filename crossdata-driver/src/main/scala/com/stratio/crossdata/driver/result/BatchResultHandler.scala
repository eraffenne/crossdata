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

package com.stratio.crossdata.driver.result


import com.stratio.crossdata.common.result._
import com.stratio.crossdata.driver.kafka.KafkaProducer
import org.apache.log4j.Logger

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.ExecutionContext.Implicits.global

/**
 * Result handler for insert using message brokers.
 */
class BatchResultHandler(queryId: String, clusterName:String, qTableName: String, rows: Iterator[String], batchSize: Integer, pause: FiniteDuration)
  extends IDriverResultHandler {

 lazy val logger = Logger.getLogger(classOf[BatchResultHandler])

  override def processAck(queryId: String, status: QueryStatus): Unit = {

  }

  override def processResult(result: Result): Unit = {
    val batchResult: PlanInsertResult = result.asInstanceOf[PlanInsertResult]
    val topic: String = batchResult.getConnector

    // TODO: Create Kafka producer using topic, rows, batchSize & pause
    val kProducer = KafkaProducer(topic)

    // TODO Create topic if not exists
    kProducer.createTopicIfNotExists()

    val insertResult = Future {
      var count = 0
      for (row <- rows) {
        count = count + 1
        kProducer.send(clusterName+"."+qTableName, row)
        if(count % batchSize == 0) {
          Thread.sleep(pause.toMillis)
        }
      }
    }

    insertResult onFailure {
      case error => logger.error (s"An error has occured: ${error.getMessage}")
    }

    insertResult onSuccess {
      case _ => logger.info ("Data inserted into kafka successfully")
    }

    insertResult onComplete  {
      case _ => {
        logger.info ("Insert completed")
        kProducer.close()
      }
    }

  }

  override def processError(errorResult: Result): Unit = {
    implicit def resToError(p: Result) = p.asInstanceOf[ErrorResult]
    logger.error(s"Error: ${errorResult.getErrorMessage}")
  }



}
