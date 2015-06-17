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

package com.stratio.crossdata.driver.kafka

import com.stratio.crossdata.driver.config.DriverConfig
import kafka.admin.AdminUtils
import kafka.producer._
import java.io.Closeable
import java.util.Properties

import kafka.utils.ZKStringSerializer
import org.I0Itec.zkclient.ZkClient
import org.apache.log4j.Logger

object KafkaProducer {
  val ZkSessionTimeoutMs = 10000
  val ZkConnectionTimeoutMs = 10000
}

case class KafkaProducer(topic: String) extends DriverConfig with Closeable {

  val props = new Properties()
  override lazy val logger = Logger.getLogger(classOf[KafkaProducer])

  props.put("metadata.broker.list", brokerList)
  //props.put("request.required.acks", new Integer(requiredAcks))
  props.put("producer.type", producerType)
  props.put("serializer.class", "kafka.serializer.StringEncoder") //TODO set => props.put("serializer.class", "kafka.serializer.DefaultEncoder")
  props.put("key.serializer.class", "kafka.serializer.StringEncoder")
  //TODO add more options (compression.codec, maxRetries, partitioner, timeouts, clientid)

  val producer = new Producer[String, String](new ProducerConfig(props)) //new Producer[String, Array[Byte]](new ProducerConfig(props))

  def send(key: String, message: String) = {
    try {
      logger.info("Sending KeyedMessage[key, value]: [" + key + "," + message + "]")
      producer.send(new KeyedMessage(topic, key, message /*message.getBytes("UTF-8")*/))
    } catch {
      case e: Exception =>
        logger.error("Error sending KeyedMessage[key, value]: [" + key + "," + message + "]")
        logger.error("Exception: " + e.getMessage)
    }
  }

  def createTopic(partitions: Int = 1, replicationFactor: Int = 1, properties: Properties = new Properties()) = {
    logger.info(s"Creating new topic: $topic with #partitions=$partitions and replicationFactor=$replicationFactor")
    val zkClient = new ZkClient(zookeeperServer)
    AdminUtils.createTopic(zkClient, topic, partitions, replicationFactor, properties)
    zkClient.close
  }

  def createTopicIfNotExists(partitions: Int = 1, replicationFactor: Int = 1, properties: Properties = new Properties()) = {
    //val zkClient = new ZkClient(zookeeperServer)

    val zkClient = new ZkClient(zookeeperServer, KafkaProducer.ZkSessionTimeoutMs, KafkaProducer.ZkConnectionTimeoutMs, ZKStringSerializer)
    if (!AdminUtils.topicExists(zkClient, topic)) {
      logger.info(s"Creating new topic: $topic with #partitions=$partitions and replicationFactor=$replicationFactor")
      AdminUtils.createTopic(zkClient, topic, partitions, replicationFactor, properties)
    } else {
      logger.info(s"Topic $topic already exists")
    }
    zkClient.close
  }

  override def close(): Unit = {
    producer.close
  }
}