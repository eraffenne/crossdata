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
package com.stratio.crossdata.connectors

import Mocks.DummyIConnector
import akka.actor.{ActorRef, ActorSystem}
import akka.agent.Agent
import akka.pattern.ask
import akka.routing.RoundRobinRouter
import akka.util.Timeout
import com.stratio.crossdata.common.connector.ObservableMap
import com.stratio.crossdata.common.data._
import com.stratio.crossdata.common.metadata._
import com.stratio.crossdata.common.result.MetadataResult
import com.stratio.crossdata.common.statements.structures.Selector
import com.stratio.crossdata.communication.{CreateTable, UpdateMetadata}
import com.stratio.crossdata.connectors.config.ConnectConfig
import org.apache.log4j.Logger
import org.scalamock.scalatest.MockFactory
import org.scalatest.{FunSuite, Suite}

import scala.collection.mutable.{ListMap, Set}
import scala.concurrent.Await
import scala.concurrent.duration.DurationInt
import scala.io.Source

object ConnectorWorkerActorTest

class ConnectorWorkerActorTest extends FunSuite with ConnectConfig with MockFactory {
  this: Suite =>

  override lazy val logger = Logger.getLogger(classOf[ConnectorWorkerActorTest])
  lazy val system1 = ActorSystem(clusterName, config)
  implicit val timeout = Timeout(10 seconds)
  val myconnector: String="myConnector"
  val myluster:String="cluster"
  val mycatalog:String="catalog"
  val mytable:String="mytable"
  val sm2:String="2"
  val a:Option[java.util.Map[Selector, Selector]] = Some(new java.util.HashMap[Selector, Selector]())
  val b:Option[java.util.LinkedHashMap[ColumnName,ColumnMetadata]] = Some(new java.util.LinkedHashMap[ColumnName, ColumnMetadata]())
  val c:Option[java.util.Map[IndexName,IndexMetadata]] = Some(new java.util.HashMap[IndexName, IndexMetadata]())
  val d:Option[ClusterName]= Some(new ClusterName(myluster))
  val e:Option[java.util.LinkedList[ColumnName]]=Some(new java.util.LinkedList[ColumnName]())

  test("Send 2 slow MetadataInProgressQuery to two connectors to test concurrency") {
    val queryId = "queryId"

    val m = new DummyIConnector()
    val m2 =new DummyIConnector()

    val connectedServers = Agent(Set.empty[String])(system1.dispatcher)
    val agent = Agent(new ObservableMap[Name, UpdatableMetadata])(system1.dispatcher)
    val runningJobsAgent = Agent(new ListMap[String, ActorRef])(system1.dispatcher)

    val ca1 = system1.actorOf(ConnectorWorkerActor.props( m, agent, runningJobsAgent))
    val ca2 = system1.actorOf(ConnectorWorkerActor.props( m2, agent, runningJobsAgent))


    val message = CreateTable(queryId, new ClusterName(myluster), new TableMetadata(new TableName(mycatalog, mytable),
      a.get, b.get, c.get, d.get, e.get, e.get))
    val message2 = CreateTable(queryId + sm2, new ClusterName(myluster), new TableMetadata(new TableName(mycatalog, mytable),
      a.get, b.get, c.get, d.get, e.get, e.get))

    logger.debug("sending message 1")
    var future = ask(ca1, message)
    logger.debug("sending message 2")
    var future2 = ask(ca2, message2)
    logger.debug("messages sent")

    val result = Await.result(future, 12 seconds).asInstanceOf[MetadataResult]
    logger.debug(" result.getQueryId()=" + result.getQueryId())
    assert(result.getQueryId() == queryId)

    val result2 = Await.result(future2, 16 seconds).asInstanceOf[MetadataResult]
    logger.debug("result.getQueryId()=" + result2.getQueryId())
    assert(result2.getQueryId() == queryId + sm2)
  }

  test("Send MetadataInProgressQuery to Connector") {
    val queryId = "queryId"
    val m=new DummyIConnector()
    val m2=new DummyIConnector()
    val connectedServers = Agent(Set.empty[String])(system1.dispatcher)
    val agent = Agent(new ObservableMap[Name, UpdatableMetadata])(system1.dispatcher)
    val runningJobsAgent = Agent(new ListMap[String, ActorRef])(system1.dispatcher)

    val ca1 = system1.actorOf(ConnectorWorkerActor.props( m,  agent, runningJobsAgent))
    val ca2 = system1.actorOf(ConnectorWorkerActor.props(m2,  agent, runningJobsAgent))
    val routees = Vector[ActorRef](ca1, ca2)

    val connectorActor = system1.actorOf(ConnectorWorkerActor.props( m, agent, runningJobsAgent).withRouter(RoundRobinRouter(routees = routees)))

    val message = CreateTable(queryId, new ClusterName(myluster), new TableMetadata(new TableName(mycatalog, mytable),
      a.get, b.get, c.get, d.get, e.get, e.get))
    val message2 = CreateTable(queryId + sm2, new ClusterName(myluster), new TableMetadata(new TableName(mycatalog, mytable),
      a.get, b.get, c.get, d.get, e.get, e.get))
    /**
     * Time to wait the make the test
     * */
    val timesleep:Int=3000
    Thread.sleep(timesleep)
    logger.debug("sending message 1")
    val future = ask(connectorActor, message)
    logger.debug("sending message 2")
    val future2 = ask(connectorActor, message2)
    logger.debug("messages sent")

    val result = Await.result(future, 12 seconds).asInstanceOf[MetadataResult]
    logger.debug("result.getQueryId() =" + result.getQueryId())
    assert(result.getQueryId() == queryId)

    val result2 = Await.result(future2, 16 seconds).asInstanceOf[MetadataResult]
    logger.debug("result.getQueryId()= " + result2.getQueryId())
    assert(result2.getQueryId() == queryId + sm2)

  }

  test("Send updateMetadata to Connector") {
    //It's not possible mock because the remove is ambiguous for scalamock
    class ObservableMapMock extends ObservableMap[Name, UpdatableMetadata] {
      var testVals = List.empty[String]
      override def put(key: Name, value: UpdatableMetadata): UpdatableMetadata = {
        testVals = key.toString+value.toString :: testVals
        null
        }
    }
    val observableMapMock = new ObservableMapMock() .asInstanceOf[ObservableMap[Name, UpdatableMetadata]]

    val agent = Agent(observableMapMock)(system1.dispatcher)
    val runningJobsAgent = Agent(new ListMap[String, ActorRef])(system1.dispatcher)
    val m=new DummyIConnector()
    val ca1 = system1.actorOf(ConnectorWorkerActor.props( m,agent, runningJobsAgent))

    val table=new TableMetadata(new TableName("catalog","name"),null,null,null,null,null,null)
    //Expectations(observableMock.put _).expects(table.getName, table)
    ca1 ! UpdateMetadata(table, false)
    Thread.sleep(200)
    assert( observableMapMock.asInstanceOf[ObservableMapMock].testVals.size == 1, "The table metadata has not been updated")
    expectResult(table.getName.toString+table.toString)( observableMapMock.asInstanceOf[ObservableMapMock].testVals.head)

  }

}


