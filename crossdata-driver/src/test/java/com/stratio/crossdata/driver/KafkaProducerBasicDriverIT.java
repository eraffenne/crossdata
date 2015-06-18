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

package com.stratio.crossdata.driver;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;

import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.log4j.Logger;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import com.stratio.connector.inmemory.InMemoryConnector;
import com.stratio.crossdata.common.manifest.CrossdataManifest;
import com.stratio.crossdata.common.result.CommandResult;
import com.stratio.crossdata.common.result.ConnectResult;
import com.stratio.crossdata.common.result.ConnectToConnectorResult;
import com.stratio.crossdata.common.result.ErrorResult;
import com.stratio.crossdata.common.result.InProgressResult;
import com.stratio.crossdata.common.result.MetadataResult;
import com.stratio.crossdata.common.result.PlanInsertResult;
import com.stratio.crossdata.common.result.Result;
import com.stratio.crossdata.connectors.ConnectorApp;
import com.stratio.crossdata.connectors.kafka.KafkaConnectorApp;
import com.stratio.crossdata.server.CrossdataServer;

import akka.actor.ActorRef;
import scala.concurrent.duration.FiniteDuration;

public class KafkaProducerBasicDriverIT {

    /**
     * Class logger.
     */
    private static final Logger LOG = Logger.getLogger(KafkaProducerBasicDriverIT.class);

    private CrossdataServer server;
    private KafkaConnectorApp connector;
    private BasicDriver driver;

    @BeforeClass
    public void setUp() throws Exception {
        server = new CrossdataServer();
        server.init(null);
        server.start();
        Thread.sleep(5000);
        connector = new KafkaConnectorApp();
        InMemoryConnector inMemoryConnector = new InMemoryConnector(connector);
        ActorRef actorSelection = connector.startup(inMemoryConnector).get();
        Thread.sleep(5000);
    }

    @AfterClass
    public void clean(){
        driver.resetServerdata();

    }

    @Test(timeOut = 8000)
    public void testConnect() throws Exception {
        driver = new BasicDriver();
        driver.setUserName("ITtest");
        Result result = driver.connect(driver.getUserName(), driver.getPassword());

        if(result instanceof ErrorResult){
            LOG.error(((ErrorResult) result).getErrorMessage());
        }
        assertFalse(result.hasError(), "Server returned an error");
        assertEquals(result.getClass(), ConnectResult.class, "ConnectResult was expected");
        ConnectResult connectResult = (ConnectResult) result;
        assertNotNull(connectResult.getSessionId(), "Server returned a null session identifier");
        LOG.info(connectResult.getSessionId());
    }

    @Test(timeOut = 8000, dependsOnMethods = {"testConnect"})
    public void testResetServerdata() throws Exception {
        Thread.sleep(500);
        Result result = driver.resetServerdata();

        if(result instanceof ErrorResult){
            LOG.error(((ErrorResult) result).getErrorMessage());
        }
        assertFalse(result.hasError(), "Server returned an error");
        assertEquals(result.getClass(), CommandResult.class, "CommandResult was expected");
        CommandResult commandResult = (CommandResult) result;
        assertNotNull(commandResult.getResult(), "Server returned a null object");
        LOG.info(commandResult.getResult());
    }

    @Test(timeOut = 8000, dependsOnMethods = {"testResetServerdata"})
    public void testAddDatastore() throws Exception {
        Thread.sleep(500);
        URL url = this.getClass().getClassLoader().getResource("InMemoryDataStore.xml");

        String path = url.getPath();
        Result result = driver.addManifest(CrossdataManifest.TYPE_DATASTORE, path);

        if(result instanceof ErrorResult){
            LOG.error(((ErrorResult) result).getErrorMessage());
        }
        assertFalse(result.hasError(), "Server returned an error");
        assertEquals(result.getClass(), CommandResult.class, "CommandResult was expected");
        CommandResult commandResult = (CommandResult) result;
        assertNotNull(commandResult.getResult(), "Server returned a null object");
        LOG.info(commandResult.getResult());
    }

    @Test(timeOut = 8000, dependsOnMethods = {"testAddDatastore"})
    public void testAttachCluster() throws Exception {
        Thread.sleep(500);
        Result result = driver.executeQuery("ATTACH CLUSTER InMemoryCluster ON DATASTORE InMemoryDatastore WITH " +
                "OPTIONS {'TableRowLimit': 100};");

        if(result instanceof ErrorResult){
            LOG.error(((ErrorResult) result).getErrorMessage());
        }
        assertFalse(result.hasError(), "Server returned an error");
        assertEquals(result.getClass(), CommandResult.class, "CommandResult was expected");
        CommandResult commandResult = (CommandResult) result;
        assertNotNull(commandResult.getResult(), "Server returned a null object");
        LOG.info(commandResult.getResult());
    }

    @Test(timeOut = 8000, dependsOnMethods = {"testAttachCluster"})
    public void testAddConnector() throws Exception {
        Thread.sleep(500);
        URL url = Thread.currentThread().getContextClassLoader().getResource("InMemoryConnector.xml");
        //String path = url.getPath().replace("crossdata-driver", "crossdata-connector-inmemory");
        String path = url.getPath();
        Result result = driver.addManifest(CrossdataManifest.TYPE_CONNECTOR, path);

        if(result instanceof ErrorResult){
            LOG.error(((ErrorResult) result).getErrorMessage());
        }
        assertFalse(result.hasError(), "Server returned an error");
        assertEquals(result.getClass(), CommandResult.class, "CommandResult was expected");
        CommandResult commandResult = (CommandResult) result;
        assertNotNull(commandResult.getResult(), "Server returned a null object");
        LOG.info(commandResult.getResult());
    }

    @Test(timeOut = 8000, dependsOnMethods = {"testAddConnector"})
    public void testAttachConnector() throws Exception {
        Thread.sleep(500);
        Result result = driver.executeQuery("ATTACH CONNECTOR InMemoryConnector TO InMemoryCluster;");

        if(result instanceof ErrorResult){
            LOG.error(((ErrorResult) result).getErrorMessage());
        }
        assertFalse(result.hasError(), "Server returned an error");
        assertEquals(result.getClass(), ConnectToConnectorResult.class, "CommandResult was expected");
        ConnectToConnectorResult connectResult = (ConnectToConnectorResult) result;
        assertNotNull(connectResult.getQueryId(), "Server returned a null session identifier");
        LOG.info(connectResult.getQueryId());
    }

    @Test(timeOut = 8000, dependsOnMethods = {"testAttachConnector"})
    public void testCreateCatalog() throws Exception {
        Thread.sleep(500);
        Result result = driver.executeQuery("CREATE CATALOG catalogTest;");

        if(result instanceof ErrorResult){
            LOG.error(((ErrorResult) result).getErrorMessage());
        }
        assertFalse(result.hasError(), "Server returned an error");
        assertEquals(result.getClass(), MetadataResult.class, "CommandResult was expected");
        MetadataResult metadataResult = (MetadataResult) result;
        assertNotNull(metadataResult.getOperation(), "Server returned a null object");
        LOG.info(metadataResult.getOperation());
        Thread.sleep(500);
        driver.setCurrentCatalog("catalogTest");
    }

    @Test(timeOut = 8000, dependsOnMethods = {"testCreateCatalog"})
    public void testCreateTable() throws Exception {
        Thread.sleep(500);
        Result result = driver.executeQuery("CREATE TABLE tableTest ON CLUSTER InMemoryCluster" +
                " (id INT PRIMARY KEY, name TEXT, description TEXT, rating FLOAT);");

        if(result instanceof ErrorResult){
            LOG.error(((ErrorResult) result).getErrorMessage());
        }
        assertFalse(result.hasError(), "Server returned an error");
        assertEquals(result.getClass(), MetadataResult.class, "CommandResult was expected");
        MetadataResult metadataResult = (MetadataResult) result;
        assertNotNull(metadataResult.getOperation(), "Server returned a null object");
        LOG.info(metadataResult.getOperation());
    }

    @Test(/*timeOut = 8000,*/ dependsOnMethods = {"testCreateTable"})
    public void testInsert1() throws Exception {
        Thread.sleep(500);

        String row = " stratio crossdata ";
        List<String> rowList = new ArrayList<>();
        Random random = new Random();

        for(int i = 0; i< 10000; i++){
            rowList.add(i + row + random.nextInt(6));
        }


	//ProducerRate=>100.000 msg/sg
        Result result = driver.insertRows(
                        "InMemoryCluster",
                        "tableTest",
                        Arrays.asList("id", "name", "description", "rating"),
                        rowList.iterator(),
                        100,
                        new FiniteDuration(1, TimeUnit.MILLISECONDS)
                        );

        if(result instanceof ErrorResult){
            LOG.error(((ErrorResult) result).getErrorMessage());
        }
        assertFalse(result.hasError(), "Server returned an error");
        assertEquals(result.getClass(), InProgressResult.class, "InProgressResult was expected");
        InProgressResult ipResult = (InProgressResult) result;
        LOG.info("Execution in progress with queryId: " + ipResult.getQueryId());

        Thread.sleep(10000);
    }

    @AfterClass
    public void tearDown() throws Exception {
        connector.stop();
        server.stop();
        server.destroy();
    }

}
