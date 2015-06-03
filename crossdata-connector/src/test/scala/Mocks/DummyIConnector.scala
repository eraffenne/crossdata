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

package Mocks


import com.stratio.crossdata.common.connector._
import com.stratio.crossdata.common.data.ClusterName
import com.stratio.crossdata.common.security.ICredentials

class DummyIConnector extends IConnector{
  override def shutdown: Unit = {}
  override def init(configuration: IConfiguration):Unit ={}
  override def getMetadataEngine: IMetadataEngine = new DummyIMetadataEngine
  val b:Option[IQueryEngine]=None
  override def getQueryEngine: IQueryEngine = b.get
  override def isConnected(name: ClusterName): Boolean = false
  override def close(name: ClusterName): Unit = {}
  override def connect(credentials: ICredentials, config: ConnectorClusterConfig): Unit = {}
  val c:Option[IStorageEngine]=None
  override def getStorageEngine: IStorageEngine = c.get
  override def restart = ()
  override def getConnectorManifestPath: String = "connectorPath"
  override def getDatastoreManifestPath: Array[String] = Array("datastorePath")
}
