/*
 *
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
 *
 *
 */

package com.stratio.crossdata.core.statements;

import java.util.ArrayList;
import java.util.List;

import com.stratio.crossdata.common.data.CatalogName;
import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.data.TableName;
import com.stratio.crossdata.core.validator.requirements.ValidationRequirements;
import com.stratio.crossdata.core.validator.requirements.ValidationTypes;

public class InsertBatchStatement extends StorageStatement {

    private TableName tableName;
    private final List<ColumnName> columnNames = new ArrayList<>();

    public InsertBatchStatement(String qualifiedTable, List<String> columnNames) {
        this.command = false;
        String catalog = qualifiedTable.split(".")[0];
        this.catalogInc = true;
        this.catalog = new CatalogName(catalog);
        String table = qualifiedTable.split(".")[1];
        this.tableName = new TableName(catalog, table);
        for(String columnName: columnNames){
            this.columnNames.add(new ColumnName(catalog, table, columnName));
        }
    }

    public TableName getTableName() {
        return tableName;
    }

    public List<ColumnName> getColumnNames() {
        return columnNames;
    }

    @Override
    public ValidationRequirements getValidationRequirements() {
        return new ValidationRequirements()
                .add(ValidationTypes.MUST_EXIST_TABLE)
                .add(ValidationTypes.MUST_EXIST_COLUMN);
    }

    @Override
    public String toString() {
        return "INSERT_BATCH(" + tableName + ", " + columnNames + ")";
    }

}
