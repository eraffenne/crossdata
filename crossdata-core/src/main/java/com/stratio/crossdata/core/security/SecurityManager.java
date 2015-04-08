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
package com.stratio.crossdata.core.security;

import java.io.Serializable;
import java.util.List;

/**
 * Singleton to manage the security.
 */
public enum SecurityManager {

    /**
     * Security engine as enumeration to define a Singleton.
     */
    MANAGER;

    /**
     * Whether the manager has been initialized.
     */
    private boolean isInit = false;

    /**
     * The instance of the security module class.
     */
    private ISecurity security;

    /**
     * Initialize the MetadataManager. This method is mandatory to use the MetadataManager.
     */
    public synchronized void init(String securityClass) {
        try {
            security=(ISecurity)Class.forName(securityClass).newInstance();
            this.isInit = true;
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            this.isInit = false;

        }

    }

    /**
     * Check that the metadata manager has been previously initialized.
     */
    private void shouldBeInit() {
        if (!isInit) {
            throw new SecurityManagerException("Metadata is not initialized yet.");
        }
    }

    /**
     * Check the query.
     * @param sessionId
     * @param service
     * @param catalogs
     * @param tables
     * @return
     */
    public boolean isPermitted(Serializable sessionId, String service, List<String> catalogs, List<String> tables ) {
        shouldBeInit();
        return security.isPermitted(sessionId,service,catalogs, tables);
    }

    /**
     * Logout the client.
     * @param sessionId
     * @param service
     * @return
     */
    public boolean logout(Serializable sessionId, String service) {
        return security.logout(sessionId,service);
    }

    /**
     * Client login.
     * @param service
     * @return
     */
    public boolean login(String user, String password, String service) {
        return security.login(user,password,service);
    }
}
