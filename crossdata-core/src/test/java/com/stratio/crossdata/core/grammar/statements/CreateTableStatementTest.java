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

package com.stratio.crossdata.core.grammar.statements;

import org.testng.annotations.Test;

import com.stratio.crossdata.core.grammar.ParsingTest;

public class CreateTableStatementTest extends ParsingTest {

    //
    // CREATE TABLE
    //

    @Test
    public void createTableBasic() {
        String inputText = "[myCatalog], CREATE TABLE myTable ON CLUSTER siliconValley (something text PRIMARY KEY, " +
                "something2 int, something3 boolean);";
        String expectedText = "CREATE TABLE mycatalog.myTable ON CLUSTER cluster.siliconvalley(mycatalog.myTable" +
                ".something=text, mycatalog.myTable.something2=int, mycatalog.myTable.something3=boolean, PRIMARY KEY((mycatalog.myTable.something)));";
        testRegularStatement(inputText, expectedText, "createTableBasic");
    }

    @Test
    public void createTableBasicWithFloat() {
        String inputText = "[myCatalog], CREATE TABLE myTable ON CLUSTER siliconValley (something text PRIMARY KEY, " +
                "something2 int, something3 float, something4 boolean);";
        String expectedText = "CREATE TABLE mycatalog.myTable ON CLUSTER cluster.siliconvalley(mycatalog.myTable" +
                ".something=text, mycatalog.myTable.something2=int, mycatalog.myTable.something3=float, " +
                "mycatalog.myTable.something4=boolean, PRIMARY KEY((mycatalog.myTable.something)));";
        testRegularStatement(inputText, expectedText, "createTableBasic");
    }

    @Test
    public void createTableBasic2() {
        String inputText = "CREATE TABLE myTable ON CLUSTER siliconValley (something text, something2 int PRIMARY KEY, something3 boolean);";
        testParserFails("demo", inputText, "createTableBasic2");
    }

    @Test
    public void createTableBasic3() {
        String inputText = "CREATE TABLE business.myTable ON CLUSTER siliconValley (something text, something2 int, something3 boolean PRIMARY KEY);";
        testParserFails("demo", inputText, "createTableBasic3");
    }

    @Test
    public void createTableBasic4() {
        String inputText = "CREATE TABLE myTable ON CLUSTER siliconValley (something text, something2 int, something3 boolean, PRIMARY KEY (something));";
        String expectedText = "CREATE TABLE demo.myTable ON CLUSTER cluster.siliconValley(demo.myTable.something=text, demo.myTable.something2=int, demo.myTable.something3=boolean, PRIMARY KEY((demo.myTable.something)));";
        testRegularStatementSession("demo", inputText, expectedText, "createTableBasic4");
    }

    @Test
    public void createTableBasic5() {
        String inputText = "CREATE TABLE myTable ON CLUSTER siliconValley (something text, something2 int, something3 boolean, PRIMARY KEY (something, something2));";
        String expectedText = "CREATE TABLE demo.myTable ON CLUSTER cluster.siliconValley(demo.myTable.something=text, demo.myTable.something2=int, demo.myTable.something3=boolean, PRIMARY KEY((demo.myTable.something), demo.myTable.something2));";
        testRegularStatementSession("demo", inputText, expectedText, "createTableBasic5");
    }

    @Test
    public void createTableBasic6() {
        String inputText = "CREATE TABLE myTable ON CLUSTER siliconValley (something text, something2 int, something3 boolean, PRIMARY KEY ((something, something2), something3));";
        String expectedText = "CREATE TABLE demo.myTable ON CLUSTER cluster.siliconValley(demo.myTable.something=text, demo.myTable.something2=int, demo.myTable.something3=boolean, PRIMARY KEY((demo.myTable.something, demo.myTable.something2), demo.myTable.something3));";
        testRegularStatementSession("demo", inputText, expectedText, "createTableBasic6");
    }

    @Test
    public void createTableBasic7() {
        String inputText =
                "CREATE TABLE myTable ON CLUSTER siliconValley (something text, something2 int, something3 boolean, PRIMARY KEY ((something, something2), something3)) "
                        + "WITH {'propiedad1':'prop1', 'propiedad2':2, 'propiedad3':3.0};";
        String expectedText =
                "CREATE TABLE demo.myTable ON CLUSTER cluster.siliconValley(demo.myTable.something=text, demo.myTable.something2=int, demo.myTable.something3=boolean, PRIMARY KEY((demo.myTable.something, demo.myTable.something2), demo.myTable.something3)) "
                        + "WITH {'propiedad1'='prop1', 'propiedad2'=2, 'propiedad3'=3.0};";
        testRegularStatementSession("demo", inputText, expectedText, "createTableBasic7");
    }

    @Test
    public void createEphemeralTable() {
        String inputText =
                "CREATE TABLE streaming.temporal ON CLUSTER siliconValley (name varchar, age int, rating double, member boolean, PRIMARY KEY (name)) "
                        + "WITH {'ephemeral': true};";
        String expectedText =
                "CREATE TABLE streaming.temporal ON CLUSTER cluster.siliconValley(streaming.temporal.name=VARCHAR, " +
                        "streaming.temporal.age=INT, streaming.temporal.rating=double, " +
                        "streaming.temporal.member=boolean, PRIMARY KEY((streaming.temporal.name))) "
                        + "WITH {'ephemeral'=true};";
        testRegularStatementSession("demo", inputText, expectedText, "createEphemeralTable");
    }

    @Test
    public void createTableWithManyProperties() {
        String inputText =
                "CREATE TABLE key_space1.users ON CLUSTER siliconValley (name varchar, password varchar, color varchar, gender varchar,"
                        + " food varchar, animal varchar, age int, code int, PRIMARY KEY ((name, gender), color, animal)) "
                        + "WITH {'compression': '{sstable_compression: DeflateCompressor, chunk_length_kb: 64}', "
                        + "'compaction': '{class: SizeTieredCompactionStrategy, min_threshold: 6}', 'read_repair_chance': 1.0};";

        String expectedText =
                "CREATE TABLE key_space1.users ON CLUSTER cluster.siliconValley(key_space1.users.name=varchar, key_space1.users.password=varchar, key_space1.users.color=varchar, key_space1.users.gender=varchar,"
                        + " key_space1.users.food=varchar, key_space1.users.animal=varchar, key_space1.users.age=int, key_space1.users.code=int, PRIMARY KEY((key_space1.users.name, key_space1.users.gender), key_space1.users.color, key_space1.users.animal)) "
                        + "WITH {'compression'='{sstable_compression: DeflateCompressor, chunk_length_kb: 64}', "
                        + "'compaction'='{class: SizeTieredCompactionStrategy, min_threshold: 6}', 'read_repair_chance'=1.0};";

        testRegularStatementSession("key_space1", inputText, expectedText, "createTableWithManyProperties");
    }

    @Test
    public void createTableCompactStorage() {
        String inputText =
                "CREATE TABLE key_space1.sblocks ON CLUSTER siliconValley (block_id varchar, subblock_id varchar, data text, PRIMARY KEY "
                        + "(block_id, subblock_id)) WITH {'COMPACT STORAGE': true};";
        String expectedText =
                "CREATE TABLE key_space1.sblocks ON CLUSTER cluster.siliconValley(key_space1.sblocks.block_id=varchar, key_space1.sblocks.subblock_id=varchar, key_space1.sblocks.data=text, PRIMARY KEY"
                        + "((key_space1.sblocks.block_id), key_space1.sblocks.subblock_id)) WITH {'COMPACT STORAGE'=true};";
        testRegularStatementSession("demo", inputText, expectedText, "createTableCompactStorage");
    }

    @Test
    public void createTableClustering() {
        String inputText =
                "CREATE TABLE key_space1.timeseries ON CLUSTER siliconValley (event_type text, insertion_time text, event text,"
                        + " PRIMARY KEY (event_type, insertion_time)) WITH {'insertion_time': 'CLUSTERING ORDER BY, DESC'};";
        String expectedText =
                "CREATE TABLE key_space1.timeseries ON CLUSTER cluster.siliconValley(key_space1.timeseries.event_type=text, key_space1.timeseries.insertion_time=text, key_space1.timeseries.event=text,"
                        + " PRIMARY KEY((key_space1.timeseries.event_type), key_space1.timeseries.insertion_time)) WITH {'insertion_time'='CLUSTERING ORDER BY, DESC'};";
        testRegularStatementSession("demo", inputText, expectedText, "createTableClustering");
    }

    @Test
    public void createTableWithProperties() {
        String inputText =
                "CREATE TABLE key_space1.test ON CLUSTER siliconValley (name varchar, color varchar, gender varchar, food varchar, "
                        + "animal varchar, PRIMARY KEY (name)) WITH {'compression': '{sstable_compression: DeflateCompressor, "
                        + "chunk_length_kb: 64}', 'compaction': '{class: SizeTieredCompactionStrategy, min_threshold: 6}', "
                        + "'read_repair_chance': 1.0};";

        String expectedText =
                "CREATE TABLE key_space1.test ON CLUSTER cluster.siliconValley(key_space1.test.name=varchar, key_space1.test.color=varchar, key_space1.test.gender=varchar, key_space1.test.food=varchar, "
                        + "key_space1.test.animal=varchar, PRIMARY KEY((key_space1.test.name))) WITH {'compression'='{sstable_compression: DeflateCompressor, "
                        + "chunk_length_kb: 64}', 'compaction'='{class: SizeTieredCompactionStrategy, min_threshold: 6}', "
                        + "'read_repair_chance'=1.0};";

        testRegularStatementSession("key_space1", inputText, expectedText, "createTableWithProperties");
    }

    @Test
    public void createTableMapColumn() {
        String inputText =
                "CREATE TABLE demo.banks ON CLUSTER siliconValley (day text, key varchar, latitude double, longitude double, name text, "
                        + "address text, tags map<text,boolean>, lucene text, PRIMARY KEY (day, key));";
        String expectedText =
                "CREATE TABLE demo.banks ON CLUSTER cluster.siliconValley(demo.banks.day=text, demo.banks.key=varchar, demo.banks.latitude=double, demo.banks.longitude=double, demo.banks.name=text, "
                        + "demo.banks.address=text, demo.banks.tags=map<text, boolean>, demo.banks.lucene=text, PRIMARY KEY((demo.banks.day), demo.banks.key));";
        testRegularStatementSession("demo", inputText, expectedText, "createTableMapColumn");
    }

    @Test
    public void createTableWrongColumnDefinition() {
        String inputText = "CREATE TABLE myTable ON CLUSTER siliconValley (something text, PRIMARY KEY ([something, something2],something3));";
        testParserFails(inputText, "createTableWrongColumnDefinition");
    }

    @Test
    public void createTableMissingPrimaryKey() {
        String inputText = "CREATE TABLE myTable ON CLUSTER siliconValley (something text, id varchar, number int);";
        testParserFails(inputText, "createTableMissingPrimaryKey");
    }

    @Test
    public void createTableWrongColumnType() {
        String inputText = "CREATE TABLE myTable ON CLUSTER siliconValley (something uuid PRIMARY KEY, id varchar, number int);";
        testParserFails(inputText, "createTableWrongColumnType");
    }

    @Test
    public void createTableWithGetCrossdataProperty() {
        String inputText =
                "CREATE TABLE key_space1.timeseries ON CLUSTER siliconValley (event_type text, insertion_time text, event text,"
                        + " PRIMARY KEY (event_type, insertion_time)) WITH {'CLUSTERING ORDER BY': 'insertion_time DESC', 'ephemeral': true};";
        String expectedText =
                "CREATE TABLE key_space1.timeseries ON CLUSTER cluster.siliconValley(key_space1.timeseries.event_type=text, key_space1.timeseries.insertion_time=text, key_space1.timeseries.event=text,"
                        + " PRIMARY KEY((key_space1.timeseries.event_type), key_space1.timeseries.insertion_time)) WITH {'CLUSTERING ORDER BY'='insertion_time DESC', 'ephemeral'=true};";
        testRegularStatementSession("demo", inputText, expectedText, "createTableWithGetCrossdataProperty");
    }

    @Test
    public void createTableWithOptions() {
        String inputText =
                "CREATE TABLE key_space1.wallet ON CLUSTER siliconValley (day text, key varchar, latitude double, longitude double, name text, "
                        + "address text, tags map<text,boolean>, lucene text, PRIMARY KEY (day, key)) WITH {'COMPACT STORAGE': true, "
                        + "'read_repair_chance': 1.0};";
        String expectedText =
                "CREATE TABLE key_space1.wallet ON CLUSTER cluster.siliconValley(key_space1.wallet.day=text, key_space1.wallet.key=varchar, key_space1.wallet.latitude=double, key_space1.wallet.longitude=double, key_space1.wallet.name=text, "
                        + "key_space1.wallet.address=text, key_space1.wallet.tags=map<text, boolean>, key_space1.wallet.lucene=text, PRIMARY KEY((key_space1.wallet.day), key_space1.wallet.key)) WITH {'COMPACT STORAGE'=true, "
                        + "'read_repair_chance'=1.0};";
        testRegularStatementSession("demo", inputText, expectedText, "createTableWithOptions");
    }

    @Test
    public void registerTableBasic() {
        String inputText = "[myCatalog], REGISTER TABLE myTable ON CLUSTER siliconValley (something text PRIMARY KEY, " +
                        "something2 int, something3 boolean);";
        String expectedText = "REGISTER TABLE mycatalog.myTable ON CLUSTER cluster.siliconvalley(mycatalog.myTable" +
                        ".something=text, mycatalog.myTable.something2=int, mycatalog.myTable.something3=boolean, PRIMARY KEY((mycatalog.myTable.something)));";
        testRegularStatement(inputText, expectedText, "registerTableBasic");
    }

    @Test
    public void createTableWithListType(){
        String inputText = "CREATE TABLE cars ON CLUSTER carsCluster" +
                "(id INT PRIMARY KEY, data LIST<TEXT>, comment TEXT);";
        String expectedText = "CREATE TABLE shop.cars ON CLUSTER cluster.carsCluster" +
                "(shop.cars.id=INT, shop.cars.data=LIST<TEXT>, shop.cars.comment=TEXT, PRIMARY KEY((shop.cars.id)));";
        testRegularStatementSession("shop", inputText, expectedText, "createTableWithListType");
    }



    @Test
    public void createTableWithColumnOptions() {
        String inputText = "[myCatalog], CREATE TABLE myTable ON CLUSTER siliconValley (something text PRIMARY KEY, " +
                "something2 int(analyzer:basic), something3 boolean);";
        String expectedText = "CREATE TABLE mycatalog.myTable ON CLUSTER cluster.siliconvalley(mycatalog.myTable" +
                ".something=text, mycatalog.myTable.something2=int(analyzer:basic), mycatalog.myTable.something3=boolean, PRIMARY KEY((mycatalog.myTable.something)));";
        testRegularStatement(inputText, expectedText, "createTableBasic");
    }

    @Test
    public void createTableWithCuplicatedColumnOptions() {
        String inputText = "[myCatalog], CREATE TABLE myTable ON CLUSTER siliconValley (something text PRIMARY KEY, " +
                "something2 int(analyzer:basic, analyzer:english), something3 boolean);";
        String expectedText = "CREATE TABLE mycatalog.myTable ON CLUSTER cluster.siliconvalley(mycatalog.myTable" +
                ".something=text, mycatalog.myTable.something2=int(analyzer:basic, analyzer:english), mycatalog.myTable.something3=boolean, PRIMARY KEY((mycatalog.myTable.something)));";
        testRegularStatement(inputText, expectedText, "createTableBasic");
    }

    @Test
    public void createTableWithMultipleColumnOptionsInTheSameColumn() {
        String inputText = "[myCatalog], CREATE TABLE myTable ON CLUSTER siliconValley (something text PRIMARY KEY, " +
                "something2 int(analyzer:basic, format:bbbb), something3 boolean);";
        String expectedText = "CREATE TABLE mycatalog.myTable ON CLUSTER cluster.siliconvalley(mycatalog.myTable" +
                ".something=text, mycatalog.myTable.something2=int(analyzer:basic, format:bbbb), mycatalog.myTable.something3=boolean, PRIMARY KEY((mycatalog.myTable.something)));";
        testRegularStatement(inputText, expectedText, "createTableBasic");
    }

    @Test
    public void createTableWithMultipleColumnOptions() {
        String inputText = "[myCatalog], CREATE TABLE myTable ON CLUSTER siliconValley (something text PRIMARY KEY, " +
                "something2 int(analyzer:basic), something3 boolean(format:b));";
        String expectedText = "CREATE TABLE mycatalog.myTable ON CLUSTER cluster.siliconvalley(mycatalog.myTable" +
                ".something=text, mycatalog.myTable.something2=int(analyzer:basic), mycatalog.myTable.something3=boolean(format:b), PRIMARY KEY((mycatalog.myTable.something)));";
        testRegularStatement(inputText, expectedText, "createTableBasic");
    }

}
