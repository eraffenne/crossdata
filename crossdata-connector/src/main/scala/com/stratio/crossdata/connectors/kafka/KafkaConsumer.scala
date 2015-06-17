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

package com.stratio.crossdata.connectors.kafka

import com.stratio.crossdata.connectors.config.ConnectConfig
import kafka.serializer.{StringDecoder, DefaultDecoder}
import java.util.Properties
import kafka.utils.Logging
import scala.collection.JavaConversions._
import kafka.consumer.{Consumer, ConsumerConfig, Whitelist}

class KafkaConsumer(topic: String,
                    groupId: String,
                    readFromBeginning: Boolean = false
                     ) extends /*Logging with*/ ConnectConfig{
  /**
   * Secondary constructor.
   */
  def this(topic: String) {
    this(topic, topic.hashCode.toString, false)
  }

  val props = new Properties()
  props.put("zookeeper.connect", zookeeperServer)
  props.put("group.id", groupId)
  props.put("auto.offset.reset", if(readFromBeginning) "smallest" else "largest")

  val connector = Consumer.create(new ConsumerConfig(props))

  val topicFilter = new Whitelist(topic)
  val stream = connector.createMessageStreamsByFilter(topicFilter, numStreams = 1, new StringDecoder(), new StringDecoder()).get(0)

  def close() {
    connector.shutdown()
  }
}
