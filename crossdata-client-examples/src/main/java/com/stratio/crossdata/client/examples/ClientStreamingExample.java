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

package com.stratio.crossdata.client.examples;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.DecimalFormat;

import org.apache.log4j.Logger;
import org.jfairy.Fairy;
import org.jfairy.producer.BaseProducer;
import org.jfairy.producer.person.Person;

import com.stratio.crossdata.client.examples.utils.DriverResultHandler;
import com.stratio.crossdata.common.exceptions.ConnectionException;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.ManifestException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.exceptions.ValidationException;
import com.stratio.crossdata.common.manifest.CrossdataManifest;
import com.stratio.crossdata.common.result.Result;
import com.stratio.crossdata.driver.BasicDriver;
import com.stratio.crossdata.driver.utils.ManifestUtils;

public class ClientStreamingExample {
	/**
	 * Class constructor.
	 */
	private ClientStreamingExample() {
	}

	static final Logger LOG = Logger.getLogger(ClientStreamingExample.class);

	static final String STREAMING_DATASTORE_MANIFEST = "https://raw.githubusercontent.com/Stratio/stratio-connector-streaming/branch-0.4/connector-streaming/src/main/config/StreamingDataStore.xml";
	static final String STREAMING_CONNECTOR_MANIFEST = "https://raw.githubusercontent.com/Stratio/stratio-connector-streaming/branch-0.4/connector-streaming/src/main/config/StreamingConnector.xml";

	static final String STREAMING_DATASTORE_FILE = STREAMING_DATASTORE_MANIFEST
			.substring(STREAMING_DATASTORE_MANIFEST.lastIndexOf('/') + 1);
	static final String STREAMING_CONNECTOR_FILE = STREAMING_CONNECTOR_MANIFEST
			.substring(STREAMING_CONNECTOR_MANIFEST.lastIndexOf('/') + 1);

	static final int NUMBER_OF_ROWS = 1000;
	static final String USER_NAME = "stratio";
	static final String PASSWORD = "stratio";

	public static void main(String[] args) {

		downloadManifest(STREAMING_DATASTORE_MANIFEST, STREAMING_DATASTORE_FILE);
		downloadManifest(STREAMING_CONNECTOR_MANIFEST, STREAMING_CONNECTOR_FILE);

		BasicDriver basicDriver = new BasicDriver();

		Result result = null;
		try {
			result = basicDriver.connect(USER_NAME, PASSWORD);
		} catch (ConnectionException ex) {
			LOG.error(ex);
		}
		assert result != null;
		LOG.info("Connected to Crossdata Server");

		// RESET SERVER DATA
		result = basicDriver.resetServerdata("testSession");
		assert result != null;
		LOG.info("Server data cleaned");

		// ADD STREAMING DATASTORE MANIFEST
		CrossdataManifest manifest = null;
		try {
			manifest = ManifestUtils.parseFromXmlToManifest(
					CrossdataManifest.TYPE_DATASTORE, STREAMING_DATASTORE_FILE);
		} catch (ManifestException ex) {
			LOG.error(ex);
		}
		assert manifest != null;

		result = null;
		try {
			result = basicDriver.addManifest(manifest, "testSession");
		} catch (ManifestException ex) {
			LOG.error(ex);
		}
		assert result != null;
		LOG.info("Streaming Datastore manifest added.");

		// ATTACH CLUSTER
		result = null;
		try {
			result = basicDriver
					.executeQuery(
							"ATTACH CLUSTER streamingprod ON DATASTORE Streaming WITH OPTIONS {'KafkaServer': '[127.0.0.1]', 'KafkaPort': '[9092]', 'zooKeeperServer': '[127.0.0.1]', 'zooKeeperPort': '[2181]'};",
							"testSession");
		} catch (ConnectionException | ValidationException | ExecutionException
				| UnsupportedException ex) {
			LOG.error(ex);
		}
		assert result != null;
		LOG.info("Cluster attached.");

		// ADD STRATIO STREAMING CONNECTOR MANIFEST
		manifest = null;
		try {
			manifest = ManifestUtils.parseFromXmlToManifest(
					CrossdataManifest.TYPE_CONNECTOR, STREAMING_CONNECTOR_FILE);
		} catch (ManifestException ex) {
			LOG.error(ex);
		}
		assert manifest != null;

		result = null;
		try {
			result = basicDriver.addManifest(manifest, "testSession");
		} catch (ManifestException ex) {
			LOG.error(ex);
		}
		assert result != null;
		LOG.info("Stratio Streaming Connector manifest added.");

		// ATTACH STRATIO STREAMING CONNECTOR
		result = null;
		try {
			result = basicDriver
					.executeQuery(
							"ATTACH CONNECTOR StreamingConnector TO streamingprod WITH OPTIONS {};",
							"testSession");
		} catch (ConnectionException | ValidationException | ExecutionException
				| UnsupportedException ex) {
			LOG.error(ex);
		}
		assert result != null;

		LOG.info("Stratio Cassandra connector attached.");

		// CREATE CATALOG
		result = null;
		try {
			result = basicDriver.executeQuery("CREATE CATALOG catalogTest;",
					"testSession");
		} catch (ConnectionException | ValidationException | ExecutionException
				| UnsupportedException ex) {
			LOG.error(ex);
		}
		assert result != null;
		LOG.info("Catalog created.");

		// USE
		basicDriver.setCurrentCatalog("catalogTest");

		// CREATE TABLE 1
		result = null;
		try {
			result = basicDriver
					.executeQuery(
							"CREATE TABLE tableTest ON CLUSTER streamingprod "
									+ "(id int PRIMARY KEY, timestamp TIMESTAMP, Temp DOUBLE);",
							"testSession");
		} catch (ConnectionException | ValidationException | ExecutionException
				| UnsupportedException ex) {
			LOG.error(ex);
		}
		assert result != null;
		LOG.info("Table 1 created.");
		// DRIVER RESULT HANDLER
		DriverResultHandler driverResultHandler = new DriverResultHandler();
		// USE
		basicDriver.setCurrentCatalog("catalogTest");

		basicDriver.executeAsyncRawQuery(
				"SELECT * FROM tableTest WITH WINDOW 5 SECS",
				driverResultHandler, "testSession");

		while (driverResultHandler.getNumResults() < 10) {
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		basicDriver
				.stopProcess(driverResultHandler.getQueryID(), "testSession");
		// CLOSE DRIVER
		basicDriver.close();
		LOG.info("Connection closed");
	}

	private static void downloadManifest(String url, String output) {
		URL link = null;
		try {
			link = new URL(url);
		} catch (MalformedURLException ex) {
			LOG.error(ex);
		}
		assert link != null;

		InputStream in = null;
		try {
			in = new BufferedInputStream(link.openStream());
		} catch (IOException ex) {
			LOG.error(ex);
		}
		assert in != null;

		ByteArrayOutputStream out = new ByteArrayOutputStream();
		byte[] buf = new byte[1024];
		int n;
		try {
			while (-1 != (n = in.read(buf))) {
				out.write(buf, 0, n);
			}
			out.close();
		} catch (IOException ex) {
			LOG.error(ex);
		}

		FileOutputStream fos = null;
		try {
			fos = new FileOutputStream(output);
		} catch (FileNotFoundException e) {
			LOG.error(e);
		}
		try {
			in.close();
			byte[] response = out.toByteArray();

			fos.write(response);
			fos.close();
		} catch (IOException ex) {
			LOG.error(ex);
		} finally {
			if (fos != null) {
				try {
					fos.close();
				} catch (IOException e) {
					LOG.error(e);
				}
			}
		}
	}

}
