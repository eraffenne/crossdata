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

package com.stratio.connector.inmemory;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

import java.util.HashMap;
import java.util.Map;

import org.testng.annotations.Test;

import com.codahale.metrics.Metric;
import com.stratio.crossdata.common.connector.ConnectorClusterConfig;
import com.stratio.crossdata.common.connector.IConfiguration;
import com.stratio.crossdata.common.connector.IConnector;
import com.stratio.crossdata.common.connector.IConnectorApp;
import com.stratio.crossdata.common.connector.IMetadataListener;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.data.ConnectionStatus;
import com.stratio.crossdata.common.data.TableName;
import com.stratio.crossdata.common.exceptions.ConnectionException;
import com.stratio.crossdata.common.exceptions.InitializationException;
import com.stratio.crossdata.common.metadata.TableMetadata;
import com.stratio.crossdata.common.security.ICredentials;

import scala.Option;
import scala.Some;

/**
 * Main connector class tests.
 */
public class InMemoryConnectorTest {

    @Test
    public void createConnector(){
        IConnector connector = new InMemoryConnector( dummyIConnectorApp());

        try {
            connector.init(new IConfiguration() {
                @Override public int hashCode() {
                    return super.hashCode();
                }
            });
        } catch (InitializationException e) {
            fail("Failed to init the connector", e);
        }

        Map<String,String> clusterOptions = new HashMap<>();
        Map<String,String> connectorOptions = new HashMap<>();
        clusterOptions.put("TableRowLimit", "10");
        ClusterName clusterName = new ClusterName("cluster");
        ConnectorClusterConfig config = new ConnectorClusterConfig(clusterName, connectorOptions, clusterOptions);

        try {
            connector.connect(new ICredentials() {
                @Override public int hashCode() {
                    return super.hashCode();
                }
            }, config);
        } catch (ConnectionException e) {
            fail("Cannot connect to inmemory cluster", e);
        }
    }

    private IConnectorApp dummyIConnectorApp(){
        return new IConnectorApp() {
            @Override public Option<TableMetadata> getTableMetadata(ClusterName cluster, TableName tableName,
                            int timeout) {
                return new Some<>(null);
            }

            @Override public ConnectionStatus getConnectionStatus() {
                return null;
            }

            @Override public void subscribeToMetadataUpdate(IMetadataListener metadataListener) {
            }

            @Override public Metric registerMetric(String name, Metric metric) {
                return null;
            }
        };

    }
}
