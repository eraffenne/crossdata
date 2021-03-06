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

package com.stratio.crossdata.common.data;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.stratio.crossdata.common.metadata.ColumnMetadata;
import com.stratio.crossdata.common.metadata.ColumnType;
import com.stratio.crossdata.common.metadata.DataType;

public class ResultSetTest {

    ResultSet rSet;
    private Random rand;

    @BeforeClass
    public void setUp() {
        rand = new Random();
    }

    @Test
    public void testConstructor() {
        rSet = new ResultSet();
        Assert.assertNotNull(rSet);
    }

    @Test
    public void testGetRows() {
        rSet = new ResultSet();
        rSet.add(new Row("str", new Cell(new String("comment" + rand.nextInt(100)))));
        rSet.add(new Row("int", new Cell(new Integer(rand.nextInt(50)))));
        Assert.assertEquals(rSet.getRows().size(), 2);
    }

    @Test
    public void testColDefs() {
        rSet = new ResultSet();
        rSet.setColumnMetadata(buildColumnDefinitions());

        Assert.assertEquals(rSet.getColumnMetadata().get(0).getName().getQualifiedName().toLowerCase(),
                "catalogtest.tabletest.str",
                "Invalid column name");
        Assert.assertEquals(rSet.getColumnMetadata().get(0).getColumnType().getODBCType(), "SQL_VARCHAR",
                "Invalid column class");
    }

    private List<ColumnMetadata> buildColumnDefinitions() {
        List<ColumnMetadata> columnMetadataList = new ArrayList<>();

        ColumnMetadata cmStr = new ColumnMetadata(new ColumnName("catalogTest", "tableTest", "str"), null,
                new ColumnType(DataType.VARCHAR));
        columnMetadataList.add(cmStr);

        ColumnMetadata cmInt = new ColumnMetadata(new ColumnName("catalogTest", "tableTest", "int"), null,
                new ColumnType(DataType.INT));
        columnMetadataList.add(cmInt);

        ColumnMetadata cmBool = new ColumnMetadata(new ColumnName("catalogTest", "tableTest", "bool"), null,
                new ColumnType(DataType.BOOLEAN));

        columnMetadataList.add(cmBool);

        ColumnMetadata cmLong = new ColumnMetadata(new ColumnName("catalogTest", "tableTest", "long"), null,
                new ColumnType(DataType.BIGINT));

        columnMetadataList.add(cmLong);

        return columnMetadataList;
    }

}
