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

package com.stratio.crossdata.common.connector;

import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.exceptions.ConnectionException;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.InitializationException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.security.ICredentials;

/**
 * Common interface for CROSSDATA connectors. A connector provides implementations for storage and query
 * engines. Notice that connectors do not need to provide both functionalities at the same time.
 */
public interface IConnector {

    /**
     * Get the connector manifest from the connector.
     *
     * @return A String with the path of the manifest
     */
    String getConnectorManifestPath();

    /**
     * Get the list of datastore manifests from the connector.
     *
     * @return An Array of String with the path of the manifest
     */
    String[] getDatastoreManifestPath();

    /**
     * Initialize the connector service.
     *
     * @param configuration The configuration.
     * @throws InitializationException If the connector initialization fails.
     */
    void init(IConfiguration configuration) throws InitializationException;

    /**
     * Connect to a datastore using a set of options.
     *
     * @param credentials The required credentials
     * @param config      The cluster configuration.
     * @throws ConnectionException If the connection could not be established.
     */
    void connect(ICredentials credentials, ConnectorClusterConfig config)
            throws ConnectionException;

    /**
     * Close the connection with the underlying cluster.
     *
     * @param name The Cluster name.
     * @throws ConnectionException If the close operation cannot be performed.
     */
    void close(ClusterName name) throws ConnectionException;

    /**
     * Shuts down and then close all cluster's connections.
     *
     * @throws ExecutionException If the shutdown operation cannot be performed.
     */
    void shutdown() throws ExecutionException;

    /**
     * It is called when restarting the connector service. If the connector has a listener to receive metadata updates, it must be resubscribed here.
     *
     * @throws ExecutionException If the connector restart fails.
     */
    void restart() throws ExecutionException;

    /**
     * Retrieve the connectivity status with the datastore.
     *
     * @param name The Cluster name.
     * @return Whether it is connected or not.
     */
    boolean isConnected(ClusterName name);

    /**
     * Get the storage engine.
     *
     * @return An implementation of {@link com.stratio.crossdata.common.connector.IStorageEngine}.
     * @throws UnsupportedException If the connector does not provide this functionality.
     */
    IStorageEngine getStorageEngine() throws UnsupportedException;

    /**
     * Get the query engine.
     *
     * @return An implementation of {@link com.stratio.crossdata.common.connector.IQueryEngine}.
     * @throws UnsupportedException If the connector does not provide this functionality.
     */
    IQueryEngine getQueryEngine() throws UnsupportedException;

    /**
     * Get the metadata engine.
     *
     * @return An implementation of {@link com.stratio.crossdata.common.connector.IMetadataEngine}.
     * @throws UnsupportedException If the connector does not provide this functionality.
     */
    IMetadataEngine getMetadataEngine() throws UnsupportedException;

}
