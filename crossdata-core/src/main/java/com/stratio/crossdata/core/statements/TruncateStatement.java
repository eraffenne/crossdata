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

package com.stratio.crossdata.core.statements;

import com.stratio.crossdata.common.data.CatalogName;
import com.stratio.crossdata.common.data.TableName;
import com.stratio.crossdata.core.validator.requirements.ValidationTypes;
import com.stratio.crossdata.core.validator.requirements.ValidationRequirements;

public class TruncateStatement extends StorageStatement {

    private TableName tablename;

//TODO:javadoc
    public TruncateStatement(TableName tablename) {
        this.command = false;
        this.tablename = tablename;
    }

//TODO:javadoc
    public boolean isCatalogInc() {
        return catalogInc;
    }

//TODO:javadoc
    public void setCatalogInc(boolean catalogInc) {
        this.catalogInc = catalogInc;
    }

//TODO:javadoc
    public CatalogName getCatalog() {
        return catalog;
    }

//TODO:javadoc
    public void setCatalog(CatalogName catalog) {
        this.catalog = catalog;
    }

//TODO:javadoc
    public TableName getTablename() {
        return tablename;
    }

//TODO:javadoc
    public void setTablename(TableName tablename) {
        this.tablename = tablename;
    }

    @Override
//TODO:javadoc
    public String toString() {
        StringBuilder sb = new StringBuilder("TRUNCATE ");
        if (catalogInc) {
            sb.append(catalog).append(".");
        }
        sb.append(tablename);
        return sb.toString();
    }

    @Override
//TODO:javadoc
    public ValidationRequirements getValidationRequirements() {
        return new ValidationRequirements().add(ValidationTypes.MUST_EXIST_TABLE).add(ValidationTypes.MUST_EXIST_CATALOG);
    }

}
