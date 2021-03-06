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

package com.stratio.crossdata.common.metadata;

import java.io.Serializable;
import java.util.Map;

import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.data.ConnectorName;
import com.stratio.crossdata.common.statements.structures.Selector;

/**
 * ConnectorAttachedMetadata class.
 */
public class ConnectorAttachedMetadata implements Serializable {

    private static final long serialVersionUID = -2629286778891668361L;
    private final ConnectorName connectorRef;
    private final ClusterName clusterRef;
    private final Map<Selector, Selector> properties;
    private final Integer priority;

    /**
     * Constructor class.
     * @param connectorRef The connector name.
     * @param clusterRef The cluster where the connector will be attached.
     * @param properties The properties of the connector.
     * @param priority   The priority of the connector for the associated cluster.
     */
    public ConnectorAttachedMetadata(ConnectorName connectorRef, ClusterName clusterRef,
            Map<Selector, Selector> properties, Integer priority) {
        this.connectorRef = connectorRef;
        this.clusterRef = clusterRef;
        this.properties = properties;
        this.priority = priority;
    }



    public ConnectorName getConnectorRef() {
        return connectorRef;
    }

    public ClusterName getClusterRef() {
        return clusterRef;
    }

    public Map<Selector, Selector> getProperties() {
        return properties;
    }
    public Integer getPriority() {
        return priority;
    }
}
