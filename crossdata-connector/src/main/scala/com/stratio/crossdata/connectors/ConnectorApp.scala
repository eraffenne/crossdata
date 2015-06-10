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

import java.util.concurrent.TimeUnit

import akka.util.Timeout


import akka.actor._
import akka.pattern.ask


import com.codahale.metrics._
import com.stratio.crossdata.common.connector._
import com.stratio.crossdata.common.data._
import com.stratio.crossdata.common.manifest.{ConnectorType, CrossdataManifest}
import com.stratio.crossdata.common.metadata.TableMetadata
import com.stratio.crossdata.common.utils.Metrics

import com.stratio.crossdata.connectors.config.ConnectConfig
import com.stratio.crossdata.utils.ManifestUtils
import org.apache.log4j.Logger

import scala.concurrent.Await
import scala.concurrent.duration.FiniteDuration
import scala.util.Try
import com.stratio.crossdata.communication.{GetConnectorStatus, GetTableMetadata}

object ConnectorApp {
  val GetStatusTimeout = 5
}

class ConnectorApp extends ConnectConfig with IConnectorApp {

  override lazy val logger = Logger.getLogger(classOf[ConnectorApp])

  var system: Option[ActorSystem] = null
  var connectorActor: Option[ActorRef] = None

  var connectorManifest: Option[CrossdataManifest] = None
  var datastoreManifest: Option[Array[CrossdataManifest]] = None

  def startup(connector: IConnector): Option[ActorRef] = {

    logger.info("Connector started: " + connector.getClass().getName)

    connectorManifest = Some(ManifestUtils.parseFromXmlToManifest(CrossdataManifest.TYPE_CONNECTOR, connector.getConnectorManifestPath))
    datastoreManifest = Some(connector.getDatastoreManifestPath.map(ManifestUtils.parseFromXmlToManifest(CrossdataManifest.TYPE_DATASTORE, _)))

    startSystem(connector)
    connector.init(new IConfiguration {})
    connectorActor
  }

  def stop = {
    logger.info(s"Stopping the connector app")
    system.foreach(_.shutdown())
  }

  def restart( connector: IConnector): Option[ActorRef] = {
    stop
    startSystem(connector)
    connector.restart()
    connectorActor
  }

  private def startSystem(connector: IConnector) = {
    system = Some(ActorSystem(clusterName, config))
    connectorActor = system.map( _.actorOf(Props(classOf[ConnectorActor], this, connector), "ConnectorActor"))
  }

  override def getTableMetadata(clusterName: ClusterName, tableName: TableName, timeout: Int): Option[TableMetadata] = {
    /*TODO: for querying actor internal state, only messages should be used.
      i.e.{{{
        import scala.concurrent.duration._
        val timeout: akka.util.Timeout = 2.seconds
        val response: Option[TableMetadata] =
          actorClusterNode.map(actor => Await.result((actor ? GetTableMetadata).mapTo[TableMetadata],timeout))
        response.getOrElse(throw new IllegalStateException("Actor cluster node is not initialized"))
      }}}
    */
    implicit val timeoutR: Timeout = new Timeout(FiniteDuration(timeout, TimeUnit.SECONDS))
    val future = connectorActor.get ? GetTableMetadata(clusterName,tableName)

    Try(Await.result(future.mapTo[TableMetadata],FiniteDuration(timeout, TimeUnit.SECONDS))).map{ Some (_)}.recover{
      case e: Exception => logger.debug("Error fetching the catalog metadata from the ObservableMap: "+e.getMessage); None
    }.get

  }

  /*
  TODO Review 0.4.0
  override def getCatalogMetadata(catalogName: CatalogName, timeout: Int): Option[CatalogMetadata] ={
    val future = actorClusterNode.get.?(GetCatalogMetadata(catalogName))(timeout)
    Try(Await.result(future.mapTo[CatalogMetadata],Duration.fromNanos(timeout*1000000L))).map{ Some (_)}.recover{
      case e: Exception => logger.debug("Error fetching the catalog metadata from the ObservableMap: "+e.getMessage); None
    }.get

  }
 */

  /*
  /**
   * Recover the list of catalogs associated to the specified cluster.
   * @param cluster the cluster name.
   * @param timeout the timeout in ms.
   * @return The list of catalog metadata or null if the list is not ready after waiting the specified time.
   */
  @Experimental
  override def getCatalogs(cluster: ClusterName,timeout: Int = 10000): Option[util.List[CatalogMetadata]] ={
    val future = actorClusterNode.get.ask(GetCatalogs(cluster))(timeout)
    Try(Await.result(future.mapTo[util.List[CatalogMetadata]],Duration.fromNanos(timeout*1000000L))).map{ Some (_)}.recover {
      case e: Exception => logger.debug("Error fetching the catalogs from the ObservableMap: "+e.getMessage); None
    }.get
  }
 */

  def getConnectorName: String = {
    connectorManifest.get  match {
        case connMan: ConnectorType => connMan.getConnectorName
        case _ => throw new ClassCastException
    }
  }

  def getConnectorManifest = {
    connectorManifest.get
  }

  def getDatastoreManifest = {
    datastoreManifest.get
  }

  override def getConnectionStatus: ConnectionStatus = {
  implicit val stTimeout = Timeout(FiniteDuration(ConnectorApp.GetStatusTimeout,TimeUnit.SECONDS))
    connectorActor.map[ConnectionStatus]{ cActor =>
      val future = (cActor ? GetConnectorStatus).mapTo[Boolean]
      Try(Await.result(future,FiniteDuration.apply(ConnectorApp.GetStatusTimeout, TimeUnit.SECONDS))).map{ isConnected =>
          if (isConnected) ConnectionStatus.CONNECTED else ConnectionStatus.DISCONNECTED
      }.recover{
        case e: Exception => logger.debug("Error asking for the connector status: "+e.getMessage); ConnectionStatus.DISCONNECTED
      }.get
    }.getOrElse(ConnectionStatus.DISCONNECTED)
  }


  override def subscribeToMetadataUpdate(mapListener: IMetadataListener) ={
    connectorActor.foreach(_ ! mapListener)
  }

  override def registerMetric(name: String, metric: Metric): Metric = {
    Metrics.getRegistry.register(name, metric)
  }
}
