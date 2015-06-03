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

package com.stratio.crossdata.core.api;

import static com.stratio.crossdata.common.statements.structures.SelectorHelper.convertSelectorMapToStringMap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;

import org.apache.log4j.Logger;

import com.stratio.crossdata.common.ask.APICommand;
import com.stratio.crossdata.common.ask.Command;
import com.stratio.crossdata.common.connector.ConnectorClusterConfig;
import com.stratio.crossdata.common.data.CatalogName;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.data.ConnectorName;
import com.stratio.crossdata.common.data.DataStoreName;
import com.stratio.crossdata.common.data.Status;
import com.stratio.crossdata.common.data.TableName;
import com.stratio.crossdata.common.exceptions.ApiException;
import com.stratio.crossdata.common.exceptions.IgnoreQueryException;
import com.stratio.crossdata.common.exceptions.ManifestException;
import com.stratio.crossdata.common.exceptions.ParsingException;
import com.stratio.crossdata.common.exceptions.PlanningException;
import com.stratio.crossdata.common.exceptions.ValidationException;
import com.stratio.crossdata.common.exceptions.validation.NotExistNameException;
import com.stratio.crossdata.common.executionplan.ExecutionType;
import com.stratio.crossdata.common.executionplan.ManagementWorkflow;
import com.stratio.crossdata.common.executionplan.ResultType;
import com.stratio.crossdata.common.manifest.BehaviorsType;
import com.stratio.crossdata.common.manifest.ConnectorFunctionsType;
import com.stratio.crossdata.common.manifest.ConnectorType;
import com.stratio.crossdata.common.manifest.CrossdataManifest;
import com.stratio.crossdata.common.manifest.DataStoreFunctionsType;
import com.stratio.crossdata.common.manifest.DataStoreRefsType;
import com.stratio.crossdata.common.manifest.DataStoreType;
import com.stratio.crossdata.common.manifest.FunctionType;
import com.stratio.crossdata.common.manifest.ManifestHelper;
import com.stratio.crossdata.common.manifest.PropertiesType;
import com.stratio.crossdata.common.manifest.PropertyType;
import com.stratio.crossdata.common.manifest.SupportedOperationsType;
import com.stratio.crossdata.common.metadata.CatalogMetadata;
import com.stratio.crossdata.common.metadata.ClusterAttachedMetadata;
import com.stratio.crossdata.common.metadata.ClusterMetadata;
import com.stratio.crossdata.common.metadata.ColumnMetadata;
import com.stratio.crossdata.common.metadata.ConnectorAttachedMetadata;
import com.stratio.crossdata.common.metadata.ConnectorMetadata;
import com.stratio.crossdata.common.metadata.DataStoreMetadata;
import com.stratio.crossdata.common.metadata.Operations;
import com.stratio.crossdata.common.metadata.TableMetadata;
import com.stratio.crossdata.common.result.CommandResult;
import com.stratio.crossdata.common.result.ErrorResult;
import com.stratio.crossdata.common.result.MetadataResult;
import com.stratio.crossdata.common.result.ResetServerDataResult;
import com.stratio.crossdata.common.result.Result;
import com.stratio.crossdata.common.statements.structures.Selector;
import com.stratio.crossdata.core.execution.ExecutionManager;
import com.stratio.crossdata.core.metadata.MetadataManager;
import com.stratio.crossdata.core.metadata.MetadataManagerException;
import com.stratio.crossdata.core.parser.Parser;
import com.stratio.crossdata.core.planner.Planner;
import com.stratio.crossdata.core.query.BaseQuery;
import com.stratio.crossdata.core.query.ForceDetachQuery;
import com.stratio.crossdata.core.query.IParsedQuery;
import com.stratio.crossdata.core.query.IValidatedQuery;
import com.stratio.crossdata.core.query.MetadataPlannedQuery;
import com.stratio.crossdata.core.query.MetadataValidatedQuery;
import com.stratio.crossdata.core.query.SelectPlannedQuery;
import com.stratio.crossdata.core.query.SelectValidatedQuery;
import com.stratio.crossdata.core.query.StoragePlannedQuery;
import com.stratio.crossdata.core.query.StorageValidatedQuery;
import com.stratio.crossdata.core.validator.Validator;

/**
 * Class that manages the Crossdata API requests.
 */
public class APIManager {

    /**
     * Crossdata parser.
     */
    private final Parser parser;

    /**
     * Crossdata validator.
     */
    private final Validator validator;

    /**
     * Crossdata planner.
     */
    private final Planner planner;

    /**
     * Class logger.
     */
    private static final Logger LOG = Logger.getLogger(APIManager.class);
    private static final String PROCESSING = "Processing ";

    /**
     * Constructor class.
     * @param parser The parser.
     * @param validator The validator.
     * @param planner The planner.
     */
    public APIManager(Parser parser, Validator validator, Planner planner) {
        this.parser = parser;
        this.validator = validator;
        this.planner = planner;
    }

    public String getHost(){
        return planner.getHost();
    }

    private Result processRequestListCatalogs(){
        Result result;
        LOG.info("Processing " + APICommand.LIST_CATALOGS().toString());
        LOG.info(PROCESSING + APICommand.LIST_CATALOGS().toString());
        List<CatalogMetadata> catalogsMetadata = MetadataManager.MANAGER.getCatalogs();

        result = MetadataResult.createSuccessMetadataResult(MetadataResult.OPERATION_LIST_CATALOGS);
        List<String> catalogs = new ArrayList<>();
        for (CatalogMetadata catalogMetadata: catalogsMetadata) {
            catalogs.add(catalogMetadata.getName().getName());
        }
        ((MetadataResult) result).setCatalogList(catalogs);
        return result;
    }

    private Result processRequestListTables(Command cmd){
        Result result;
        List<TableMetadata> tables;

        if (cmd.params() != null && !cmd.params().isEmpty()) {
            String catalog = (String) cmd.params().get(0);
            LOG.info(PROCESSING + APICommand.LIST_TABLES().toString());
            tables = MetadataManager.MANAGER.getTablesByCatalogName(catalog);
        } else {
            LOG.info(PROCESSING + APICommand.LIST_TABLES().toString());
            tables = MetadataManager.MANAGER.getTables();
        }
        result = MetadataResult.createSuccessMetadataResult(MetadataResult.OPERATION_LIST_TABLES);
        ((MetadataResult) result).setTableList(tables);
        return result;
    }

    private Result processRequestListColumns(Command cmd){
        Result result;
        LOG.info("Processing " + APICommand.LIST_COLUMNS().toString());
        List<ColumnMetadata> columns;

        if (cmd.params() != null && !cmd.params().isEmpty()) {
            String catalog = (String) cmd.params().get(0);
            String table = (String) cmd.params().get(1);
            columns = MetadataManager.MANAGER.getColumnByTable(catalog, table);
        } else {
            columns = MetadataManager.MANAGER.getColumns();
        }

        result = MetadataResult.createSuccessMetadataResult(MetadataResult.OPERATION_LIST_COLUMNS);
        ((MetadataResult) result).setColumnList(columns);
        return result;
    }

    private Result processRequestAddManifest(Command cmd){
        Result result;
        LOG.info(PROCESSING + APICommand.ADD_MANIFEST().toString());
        result = CommandResult.createCommandResult("CrossdataManifest added "
                + System.lineSeparator()
                + cmd.params().get(0).toString());
        try {
            persistManifest((CrossdataManifest) cmd.params().get(0));
        } catch (ApiException e) {
            result = new ErrorResult(e);
        }
        return result;
    }

    private Result processRequestDropManifest(Command cmd){
        Result result;
        LOG.info(PROCESSING + APICommand.DROP_MANIFEST().toString());
        result = CommandResult.createCommandResult("Manifest dropped");
        try {
            dropManifest(Integer.parseInt(String.valueOf(cmd.params().get(0))), cmd.params().get(1).toString());
        } catch (ApiException e) {
            LOG.info(e.getMessage(),e);
            result = CommandResult.createExecutionErrorResult(e.getMessage());
        }
        return result;
    }

    /**
     * Process an incoming API request.
     *
     * @param cmd The command to be executed.
     * @return A {@link com.stratio.crossdata.common.result.MetadataResult}.
     */
    public Result processRequest(Command cmd) {
        Result result;
        if (APICommand.LIST_CATALOGS().equals(cmd.commandType())) {
            result=processRequestListCatalogs();
        } else if (APICommand.LIST_TABLES().equals(cmd.commandType())) {
           result=processRequestListTables(cmd);
        } else if (APICommand.LIST_COLUMNS().equals(cmd.commandType())) {
            result=processRequestListColumns(cmd);
        } else if (APICommand.ADD_MANIFEST().equals(cmd.commandType())) {
            result=processRequestAddManifest(cmd);
        } else if (APICommand.DROP_MANIFEST().equals(cmd.commandType())) {
            result=processRequestDropManifest(cmd);
        } else if (APICommand.RESET_SERVERDATA().equals(cmd.commandType())) {
            LOG.info(PROCESSING + APICommand.RESET_SERVERDATA().toString());
            result = resetServerdata();
        } else if (APICommand.CLEAN_METADATA().equals(cmd.commandType())) {
            LOG.info(PROCESSING + APICommand.CLEAN_METADATA().toString());
            result = cleanMetadata();
        } else if (APICommand.DESCRIBE_CONNECTORS().equals(cmd.commandType())) {
            LOG.info(PROCESSING + APICommand.DESCRIBE_CONNECTORS().toString());
            result = describeConnectors();
        } else if (APICommand.DESCRIBE_CONNECTOR().equals(cmd.commandType())) {
            LOG.info(PROCESSING + APICommand.DESCRIBE_CONNECTOR().toString());
            result = describeConnector((ConnectorName) cmd.params().get(0));
        } else if (APICommand.DESCRIBE_CLUSTER().equals(cmd.commandType())) {
            LOG.info(PROCESSING + APICommand.DESCRIBE_CLUSTER().toString());
            result = describeCluster((ClusterName) cmd.params().get(0));
        } else if (APICommand.DESCRIBE_CATALOG().equals(cmd.commandType())) {
            LOG.info(PROCESSING + APICommand.DESCRIBE_CATALOG().toString());
            result = describeCatalog((CatalogName) cmd.params().get(0));
        } else if (APICommand.DESCRIBE_TABLE().equals(cmd.commandType())) {
            LOG.info(PROCESSING + APICommand.DESCRIBE_TABLE().toString());
            result = describeTable((TableName) cmd.params().get(0));
        } else if (APICommand.DESCRIBE_TABLES().equals(cmd.commandType())) {
            LOG.info(PROCESSING + APICommand.DESCRIBE_TABLES().toString());
            result = describeTables((CatalogName) cmd.params().get(0));
        } else if (APICommand.DESCRIBE_DATASTORES().equals(cmd.commandType())) {
            LOG.info(PROCESSING + APICommand.DESCRIBE_DATASTORES().toString());
            result = describeDatastores();
        } else if (APICommand.DESCRIBE_CLUSTERS().equals(cmd.commandType())) {
            LOG.info(PROCESSING + APICommand.DESCRIBE_CLUSTERS().toString());
            result = describeClusters();
        } else if (APICommand.DESCRIBE_DATASTORE().equals(cmd.commandType())) {
            LOG.info(PROCESSING + APICommand.DESCRIBE_DATASTORE().toString());
            result = describeDatastore((DataStoreName) cmd.params().get(0));
        } else if (APICommand.DESCRIBE_SYSTEM().equals(cmd.commandType())) {
            LOG.info(PROCESSING + APICommand.DESCRIBE_SYSTEM().toString());
            result = describeSystem();
        } else if (APICommand.EXPLAIN_PLAN().equals(cmd.commandType())) {
            LOG.info(PROCESSING + APICommand.EXPLAIN_PLAN().toString());
            result = explainPlan(cmd);
        } else{
            result = Result.createUnsupportedOperationErrorResult("Command " + cmd.commandType() + " not supported");
            LOG.error(ErrorResult.class.cast(result).getErrorMessage());
        }
        result.setQueryId(cmd.queryId());
        return result;
    }

    private Result describeSystem() {
        Result result;
        StringBuilder stringBuilder = new StringBuilder().append(System.getProperty("line.separator"));
        List<DataStoreMetadata> dataStores = MetadataManager.MANAGER.getDatastores();
        for (DataStoreMetadata dataStore : dataStores) {
            stringBuilder = stringBuilder.append("Datastore ").append(dataStore.getName())
                    .append(":").append(System.getProperty("line.separator"));
            Set<Map.Entry<ClusterName, ClusterAttachedMetadata>> refs = dataStore.getClusterAttachedRefs().entrySet();
            for (Map.Entry<ClusterName, ClusterAttachedMetadata> ref : refs) {
                ClusterName clustername = ref.getKey();
                stringBuilder = stringBuilder.append("\tCluster ").append(clustername.getName())
                        .append(":").append(System.getProperty("line.separator"));
                ClusterMetadata cluster = MetadataManager.MANAGER.getCluster(clustername);
                Set<Map.Entry<ConnectorName, ConnectorAttachedMetadata>> connectors = cluster.getConnectorAttachedRefs()
                        .entrySet();
                for (Map.Entry<ConnectorName, ConnectorAttachedMetadata> c : connectors) {
                    stringBuilder = stringBuilder.append("\t\tConnector ").append(c.getKey().getName())
                            .append(System.getProperty("line.separator"));
                }
            }
        }
        result = CommandResult.createCommandResult(stringBuilder.toString());
        return result;
    }

    private Result describeDatastore(DataStoreName dataStoreName) {
        Result result = null;
        DataStoreMetadata datastore = null;
        try {
            datastore = MetadataManager.MANAGER.getDataStore(dataStoreName);
        } catch (MetadataManagerException mme) {
            LOG.info(mme.getMessage(),mme);
            result = ErrorResult.createErrorResult(new ApiException(mme.getMessage()));
        }

        if (datastore != null) {
            StringBuilder sb = new StringBuilder(System.getProperty("line.separator"));
            sb.append("\t").append("Name: ").append(System.getProperty("line.separator")).append("\t\t").append
                    (datastore.getName()).append(System.lineSeparator());
            sb.append("\t").append("Version: ").append(System.getProperty("line.separator")).append("\t\t").append
                    (datastore.getVersion()).append(System.lineSeparator());
            sb.append("\t").append("Required properties: ").append(System.getProperty("line.separator"));
            Set<PropertyType> requiredProps = datastore.getRequiredProperties();
            for (PropertyType pt : requiredProps) {
                sb.append("\t\t").append(pt.getPropertyName()).append(": ").append(pt.getDescription()).append(
                        System.lineSeparator());
            }

            sb.append("\t").append("Other properties: ").append(System.lineSeparator());
            Set<PropertyType> othersProps = datastore.getOthersProperties();
            for (PropertyType pt : othersProps) {
                sb.append("\t\t").append(pt.getPropertyName()).append(": ").append(pt.getDescription()).append(
                        System.lineSeparator());
            }

            sb.append("\t").append("Functions: ").append(System.lineSeparator());
            Set<FunctionType> functions=datastore.getFunctions();
            for (FunctionType function:functions){
                sb.append("\t\t").append(function.getFunctionName()).append(": ").append(function.getDescription()).
                        append(System.lineSeparator());
            }

            sb.append("\t").append("Behaviours: ").append(System.getProperty("line.separator")).append("\t\t").append
                    (datastore.getBehaviors()).append(System.lineSeparator());
            sb.append("\t").append("Attached Refs: ").append(System.getProperty("line.separator")).append
                    ("\t\t").append(datastore.getClusterAttachedRefs().keySet()).append(System.lineSeparator());
            result = CommandResult.createCommandResult(sb.toString());
        }

        return result;
    }

    private Result describeConnector(ConnectorName name) {
        Result result = null;
        ConnectorMetadata connector = null;
        try {
            connector = MetadataManager.MANAGER.getConnector(name);
        } catch (MetadataManagerException mme) {
            LOG.info(mme.getMessage(),mme);
            result = ErrorResult.createErrorResult(new ApiException(mme.getMessage()));
        }

        if (connector != null) {
            StringBuilder stringBuilder = new StringBuilder().append(System.getProperty("line.separator"));
            Set<DataStoreName> datastores = connector.getDataStoreRefs();
            Set<ClusterName> clusters = connector.getClusterRefs();
            Map<ClusterName, Map<Selector, Selector>> properties = connector.getClusterProperties();
            Set<Operations> supportedOperations=connector.getSupportedOperations();
            Set<FunctionType> functions=connector.getConnectorFunctions();

            stringBuilder = stringBuilder.append("Connector: ").append(connector.getName())
                    .append("\t").append(System.getProperty("line.separator"));

            stringBuilder.append("\t").append("Status: ").append(connector.getStatus()).append(System.getProperty(
                    "line.separator"));

            stringBuilder.append("\t").append("Native: ").append(connector.isNative()).append(System.getProperty("line" +
                    ".separator"));

            stringBuilder.append("\t").append("Properties: ").append(System.getProperty("line.separator"));

            Iterator<Map.Entry<ClusterName, Map<Selector, Selector>>> propIt = properties.entrySet().iterator();
            while (propIt.hasNext()) {
                Map.Entry<ClusterName, Map<Selector, Selector>> e = propIt.next();
                stringBuilder.append("\t\t").append(e.getKey().toString()).append(": ")
                        .append(e.getValue().toString())
                        .append(System.getProperty("line.separator"));
            }

            stringBuilder.append("\t").append("Datastores: ").append(System.getProperty("line.separator"));
            for (DataStoreName datastore : datastores) {
                stringBuilder.append("\t\t").append(datastore.getName().toString())
                        .append(System.getProperty("line.separator"));
            }

            stringBuilder.append("\t").append("Clusters: ").append(System.getProperty("line.separator"));
            for (ClusterName cluster : clusters) {
                stringBuilder.append("\t\t").append(cluster.getName().toString())
                        .append(System.getProperty("line.separator"));
            }

            stringBuilder.append("\t").append("Supported Operations: ").append(System.getProperty("line.separator"));
            for (Operations operation : supportedOperations) {
                stringBuilder.append("\t\t").append(operation.getOperationsStr())
                        .append(System.getProperty("line.separator"));
            }

            stringBuilder.append("\t").append("Functions: ").append(System.getProperty("line.separator"));
            for (FunctionType function : functions) {
                stringBuilder.append("\t\t").append(function.getFunctionName()).append(": ").append(function.getDescription())
                        .append(System.getProperty("line.separator"));
            }


            stringBuilder = stringBuilder.append(System.getProperty("line.separator"));
            result = CommandResult.createCommandResult(stringBuilder.toString());
        }

        return result;
    }

    private Result describeCatalog(CatalogName name) {
        Result result;
        LOG.info("Processing " + APICommand.DESCRIBE_CATALOG().toString());
        LOG.info(PROCESSING + APICommand.DESCRIBE_CATALOG().toString());
        try {
            CatalogMetadata catalogMetadata = MetadataManager.MANAGER.getCatalog(name);
            result = MetadataResult.createSuccessMetadataResult(MetadataResult.OPERATION_DESCRIBE_CATALOG);
            ((MetadataResult) result).setCatalogMetadataList(Arrays.asList(catalogMetadata));
        }catch(MetadataManagerException e){
            LOG.info(e.getMessage());
            result=MetadataResult.createExecutionErrorResult(e.getMessage());
        }

        return result;
    }

    private Result describeTables(CatalogName name) {
        Result result;

        try {
            CatalogMetadata catalog = MetadataManager.MANAGER.getCatalog(name);
            StringBuilder sb = new StringBuilder().append(System.getProperty("line.separator"));

            sb.append("Catalog: ").append(catalog.getName()).append(System.lineSeparator());

            sb.append("Tables: ").append(catalog.getTables().keySet()).append(System.lineSeparator());

            result = CommandResult.createCommandResult(sb.toString());

        } catch (MetadataManagerException mme) {
            LOG.info(mme.getMessage(),mme);
            result = ErrorResult.createErrorResult(new ApiException(mme.getMessage()));
        }
        return result;
    }

    private Result describeTable(TableName name) {
        Result result;
        LOG.info("Processing " + APICommand.DESCRIBE_TABLE().toString());
        LOG.info(PROCESSING + APICommand.DESCRIBE_TABLE().toString());

        try {
            TableMetadata tableMetadata = MetadataManager.MANAGER.getTable(name);
            result = MetadataResult.createSuccessMetadataResult(MetadataResult.OPERATION_DESCRIBE_TABLE);
            ((MetadataResult) result).setTableList(Arrays.asList(tableMetadata));
        }catch(MetadataManagerException e){
            LOG.info(e.getMessage());
            result=MetadataResult.createExecutionErrorResult(e.getMessage());
        }

        return result;
    }

    private Result describeCluster(ClusterName name) {
        Result result;

        try {
            ClusterMetadata cluster = MetadataManager.MANAGER.getCluster(name);
            StringBuilder sb = new StringBuilder().append(System.getProperty("line.separator"));

            sb.append("Cluster: ").append(cluster.getName()).append(System.lineSeparator());

            sb.append("Datastore: ").append(cluster.getDataStoreRef()).append(System.lineSeparator());

            sb.append("Options: ").append(System.lineSeparator());
            Map<Selector, Selector> options = cluster.getOptions();
            for (Map.Entry<Selector, Selector> entry : options.entrySet()) {
                sb.append(entry.getKey()).append(": ").append(entry.getValue()).append(System.lineSeparator());
            }

            sb.append("Attached connectors: ").append(cluster.getConnectorAttachedRefs().keySet())
                    .append(System.lineSeparator());

            result = CommandResult.createCommandResult(sb.toString());
        } catch (MetadataManagerException mme) {
            LOG.info(mme.getMessage(),mme);
            result = ErrorResult.createErrorResult(new ApiException(mme.getMessage()));
        }

        return result;
    }

    private Result describeConnectors() {
        Result result;
        List<ConnectorMetadata> connectors = MetadataManager.MANAGER.getConnectors();
        StringBuilder sb = new StringBuilder().append(System.getProperty("line.separator"));

        for (ConnectorMetadata connector : connectors) {
            sb = sb.append("Connector: ").append(connector.getName())
                    .append("\t").append(connector.getStatus());
            // ClusterRefs
            if (connector.getClusterRefs() == null) {
                sb = sb.append("\t")
                        .append("UNKNOWN");
            } else {
                sb = sb.append("\t")
                        .append(Arrays.toString(connector.getClusterRefs().toArray()));
            }
            // DatastoreRefs
            if (connector.getDataStoreRefs() == null) {
                sb = sb.append("\t")
                        .append("UNKNOWN");
            } else {
                sb = sb.append("\t")
                        .append(Arrays.toString(connector.getDataStoreRefs().toArray()));
            }
            // ActorRef
            if ((connector.getActorRefs() == null) || (connector.getActorRefs().isEmpty())) {
                sb = sb.append("\t")
                        .append("UNKNOWN");
            } else {
                sb = sb.append("\t")
                        .append(connector.getActorRefs());
            }

            sb = sb.append(System.getProperty("line.separator"));
        }
        result = CommandResult.createCommandResult(sb.toString());
        return result;
    }

    private Result describeDatastores() {
        Result result;
        List<DataStoreMetadata> datastores = MetadataManager.MANAGER.getDatastores();
        StringBuilder sb = new StringBuilder().append(System.getProperty("line.separator"));
        sb.append("Datastore: ").append(System.lineSeparator());
        for (DataStoreMetadata datastore : datastores) {
            sb.append(datastore.getName()).append(System.lineSeparator());
        }
        result = CommandResult.createCommandResult(sb.toString());
        return result;
    }

    private Result describeClusters() {
        Result result;
        List<ClusterMetadata> clusters = MetadataManager.MANAGER.getClusters();
        StringBuilder sb = new StringBuilder().append(System.getProperty("line.separator"));

        for (ClusterMetadata cluster : clusters) {
            sb.append("Clusters List:").append(System.lineSeparator());
            sb.append("\t").append(cluster.getName()).append(System.lineSeparator());

        }
        result = CommandResult.createCommandResult(sb.toString());
        return result;
    }

    private Result resetServerdata() {

        Result result = null;
        try {
            //Save the connector and datastore manifest data
            List<ConnectorMetadata> connectors = MetadataManager.MANAGER.getConnectors();
            List<DataStoreMetadata> datastores = MetadataManager.MANAGER.getDatastores();

            result = createResetServerDataCommand();

            //List<ConnectorMetadata> connectors = MetadataManager.MANAGER.getConnectors();
            MetadataManager.MANAGER.clear();
            for (ConnectorMetadata cm : connectors) {
                if (cm.getStatus() == Status.ONLINE) {
                    MetadataManager.MANAGER.createConnector(cm,true);
                }
            }

            for (DataStoreMetadata datastore:datastores){
                MetadataManager.MANAGER.createDataStore(datastore);
            }

        } catch (SystemException | NotSupportedException | HeuristicRollbackException | HeuristicMixedException |
                RollbackException ex ) {
            result = CommandResult.createErrorResult(ex);
            LOG.error(ex.getMessage());
        }

        return result;
    }

    /**
     * If there are Connectors attached to Clusters, return a ResetServerDataResult with a Set of
     * ForceDetachQuery used to send a detach order to each connector.
     *
     * If there aren't connectors, return a simple CommandResult.
     *
     * @return The result after resetting the Metadata Manager.
     */
    private Result createResetServerDataCommand(){

        Collection<IParsedQuery> commands = new ArrayList<>();
        for (ClusterMetadata cluster: MetadataManager.MANAGER.getClusters()){
            for (ConnectorName connector : cluster.getConnectorAttachedRefs().keySet()){
                commands.add(createForceDetachQuery(cluster, connector));
            }
        }

        if (!commands.isEmpty()){
            ResetServerDataResult result = new ResetServerDataResult(CommandResult.createCommandResult("Crossdata server reset."));
            result.getResetCommands().addAll(commands);
            return result;
        }else{
            return CommandResult.createCommandResult("Crossdata server reset.");
        }
    }

    private ForceDetachQuery createForceDetachQuery(ClusterMetadata cluster, ConnectorName connector) {
        ClusterName clusterName = cluster.getName();
        Set<String> actorRefs = MetadataManager.MANAGER.getConnectorRefs(connector);

        Map<String, String> clusterProperties = convertSelectorMapToStringMap(MetadataManager.MANAGER.getConnector(connector).getClusterProperties().get(clusterName));
        Map<String, String> clusterOptions = convertSelectorMapToStringMap(MetadataManager.MANAGER.getCluster(clusterName).getOptions());
        ConnectorClusterConfig connectorClusterConfig = new ConnectorClusterConfig(clusterName, clusterProperties, clusterOptions);
        connectorClusterConfig.setDataStoreName(cluster.getDataStoreRef());

        ManagementWorkflow executionWorkflow = new ManagementWorkflow(UUID.randomUUID().toString(), actorRefs, ExecutionType.FORCE_DETACH_CONNECTOR, ResultType.RESULTS);
        executionWorkflow.setClusterName(clusterName);
        executionWorkflow.setConnectorName(connector);
        executionWorkflow.setConnectorClusterConfig(connectorClusterConfig);

        return new ForceDetachQuery(executionWorkflow);
    }

    private Result cleanMetadata() {
        Result result = CommandResult.createCommandResult("Metadata cleaned.");
        try {
            for (CatalogMetadata catalogMetadata : MetadataManager.MANAGER.getCatalogs()) {
                MetadataManager.MANAGER.removeCatalogFromClusters(catalogMetadata.getName());
            }
            MetadataManager.MANAGER.clearCatalogs();
            ExecutionManager.MANAGER.clear();
        } catch (SystemException | NotSupportedException | HeuristicRollbackException | HeuristicMixedException | RollbackException
                e) {
            result = CommandResult.createErrorResult(e);
            LOG.error(e.getMessage());
        }
        return result;
    }



    private void persistManifest(CrossdataManifest manifest) throws ApiException {
        try {
            if (manifest.getManifestType() == CrossdataManifest.TYPE_DATASTORE) {
                persistDataStore((DataStoreType) manifest);
            } else {
                persistConnector((ConnectorType) manifest);
            }
        } catch (NullPointerException npe) {
            throw new ApiException("Manifest couldn't be added", npe);
        }
    }

    private void persistDataStore(DataStoreType dataStoreType) throws ManifestException {
        // NAME
        DataStoreName name = new DataStoreName(dataStoreType.getName());

        /*
        *if (MetadataManager.MANAGER.exists(name)) {
        *    throw new ManifestException(new ExistNameException(name));
        *}
        */

        // VERSION
        String version = dataStoreType.getVersion();

        // REQUIRED PROPERTIES
        PropertiesType requiredProperties = dataStoreType.getRequiredProperties();

        // OPTIONAL PROPERTIES
        PropertiesType optionalProperties = dataStoreType.getOptionalProperties();

        // BEHAVIORS
        BehaviorsType behaviorsType = dataStoreType.getBehaviors();

        // DATASTORE FUNCTIONS
        DataStoreFunctionsType datastoreFunctions = dataStoreType.getFunctions();

        // Create Metadata
        DataStoreMetadata dataStoreMetadata = new DataStoreMetadata(
                name,
                version,
                (requiredProperties == null) ? null : requiredProperties.getProperty(),
                (optionalProperties == null) ? null : optionalProperties.getProperty(),
                (behaviorsType == null) ? null : behaviorsType.getBehavior(),
                (datastoreFunctions==null)? null: datastoreFunctions.getFunction()
        );


        // Persist
        MetadataManager.MANAGER.createDataStore(dataStoreMetadata, false);

        LOG.debug("DataStore added: " + MetadataManager.MANAGER.getDataStore(name).toString());

    }

    private void persistConnector(ConnectorType connectorType) throws ManifestException {
        // NAME
        ConnectorName name = new ConnectorName(connectorType.getConnectorName());

        // DATASTORES
        DataStoreRefsType dataStoreRefs = connectorType.getDataStores();

        // VERSION
        String version = connectorType.getVersion();

        // NATIVE
        Boolean isNative = connectorType.isNative();

        // REQUIRED PROPERTIES
        PropertiesType requiredProperties = connectorType.getRequiredProperties();

        // OPTIONAL PROPERTIES
        PropertiesType optionalProperties = connectorType.getOptionalProperties();

        // SUPPORTED OPERATIONS
        SupportedOperationsType supportedOperations = connectorType.getSupportedOperations();

        // CONNECTOR FUNCTIONS
        ConnectorFunctionsType connectorFunctions = connectorType.getFunctions();

        // EXCLUDED FUNCTIONS
        List<String> excludedFunctions = new ArrayList<>();
        if(connectorFunctions != null){
            Set<String> convertedExcludes = ManifestHelper.convertManifestExcludedFunctionsToMetadataExcludedFunctions(
                    connectorFunctions.getExclude());
            excludedFunctions.addAll(convertedExcludes);
        }

        // Create Metadata
        ConnectorMetadata connectorMetadata;

        if (MetadataManager.MANAGER.exists(name)) {

            connectorMetadata = MetadataManager.MANAGER.getConnector(name);

            /*
            *if (connectorMetadata.isManifestAdded()) {
            *    throw new ManifestException(new ExistNameException(name));
            *}
            */

            connectorMetadata.setVersion(version);
            connectorMetadata.setDataStoreRefs(
                    ManifestHelper.convertManifestDataStoreNamesToMetadataDataStoreNames(dataStoreRefs
                            .getDataStoreName()));
            connectorMetadata.setRequiredProperties((requiredProperties == null) ?
                    new HashSet<PropertyType>() :
                    ManifestHelper.convertManifestPropertiesToMetadataProperties(requiredProperties.getProperty()));
            connectorMetadata.setOptionalProperties((optionalProperties == null) ?
                    new HashSet<PropertyType>() :
                    ManifestHelper.convertManifestPropertiesToMetadataProperties(optionalProperties.getProperty()));

            connectorMetadata.setSupportedOperations(supportedOperations.getOperation());

            connectorMetadata.setConnectorFunctions((connectorFunctions == null) ?
                    new HashSet<FunctionType>() :
                    ManifestHelper.convertManifestFunctionsToMetadataFunctions(
                            connectorFunctions.getFunction()));

            connectorMetadata.setExcludedFunctions(
                    //(excludedFunctions == null) ?
                    //new HashSet<String>():
                    new HashSet<>(excludedFunctions));
        } else {
            connectorMetadata = new ConnectorMetadata(
                    name,
                    version,
                    isNative,
                    (dataStoreRefs == null) ? new ArrayList<String>() :  dataStoreRefs.getDataStoreName(),
                    (requiredProperties == null) ? new ArrayList<PropertyType>() : requiredProperties.getProperty(),
                    (optionalProperties == null) ? new ArrayList<PropertyType>() : optionalProperties.getProperty(),
                    (supportedOperations == null) ? new ArrayList<String>() : supportedOperations.getOperation(),
                    (connectorFunctions==null) ? new ArrayList<FunctionType>(): connectorFunctions.getFunction(),
                    excludedFunctions
            );
        }

        connectorMetadata.setManifestAdded(true);

        // Persist
        MetadataManager.MANAGER.createConnector(connectorMetadata, false);
    }

    /**
     * Remove all the information related to a Datastore or Connector.
     *
     * @param manifestType Datastore or Connector.
     * @param manifestName Name of the manifest.
     */
    private void dropManifest(int manifestType, String manifestName) throws ApiException {
        if (manifestType == CrossdataManifest.TYPE_DATASTORE) {
            dropDataStore(new DataStoreName(manifestName));
        } else {
            dropConnector(new ConnectorName(manifestName));
        }
    }

    private void dropDataStore(DataStoreName dataStoreName) throws ApiException {
        try {
            if (!MetadataManager.MANAGER.exists(dataStoreName)) {
                throw new ApiException(new NotExistNameException(dataStoreName));
            }
            MetadataManager.MANAGER.deleteDatastore(dataStoreName);
        } catch (NotSupportedException | SystemException | HeuristicRollbackException | HeuristicMixedException |
                RollbackException | MetadataManagerException e) {
            throw new ApiException(e);
        }
    }

    private void dropConnector(ConnectorName connectorName) throws ApiException {
        try {
            if (!MetadataManager.MANAGER.exists(connectorName)) {
                throw new ApiException(new NotExistNameException(connectorName));
            }
            ConnectorMetadata cm = MetadataManager.MANAGER.getConnector(connectorName);
            MetadataManager.MANAGER.deleteConnector(connectorName);
            if(cm.getStatus() == Status.ONLINE){
                String actorRef = cm.getActorRef(planner.getHost());
                MetadataManager.MANAGER.addConnectorRef(connectorName, actorRef);
                MetadataManager.MANAGER.setConnectorStatus(connectorName, Status.ONLINE);
            }
        } catch (NotSupportedException | SystemException | HeuristicRollbackException | HeuristicMixedException |
                RollbackException | MetadataManagerException e) {
            throw new ApiException(e);
        }
    }

    /**
     * Method that implements the explain logic. The method will follow the query processing stages: Parser,
     * Validator, and Planner and will provide as result the execution workflow.
     *
     * @param cmd The command to be executed.
     * @return A {@link com.stratio.crossdata.common.result.Result}.
     */
    private Result explainPlan(Command cmd) {
        Result result;
        if (cmd.params().size() == 2) {
            String statement = (String) cmd.params().get(0);
            String catalog = (String) cmd.params().get(1);

            StringBuilder plan = new StringBuilder("Explain plan for: ");
            plan.append(statement).append(System.lineSeparator());
            String realStatement=statement.substring(17);
            BaseQuery query = new BaseQuery(cmd.queryId(), realStatement, new CatalogName(catalog), cmd.sessionId());
            try {
                IParsedQuery parsedQuery = parser.parse(query);
                IValidatedQuery validatedQuery = validator.validate(parsedQuery);
                if (SelectValidatedQuery.class.isInstance(validatedQuery)) {
                    SelectPlannedQuery plannedQuery = planner.planQuery((SelectValidatedQuery) validatedQuery);
                    plan.append(plannedQuery.getExecutionWorkflow().toString());
                } else if (MetadataValidatedQuery.class.isInstance(validatedQuery)) {
                    MetadataPlannedQuery plannedQuery = planner.planQuery((MetadataValidatedQuery) validatedQuery);
                    plan.append(plannedQuery.getExecutionWorkflow());
                } else if (StorageValidatedQuery.class.isInstance(validatedQuery)) {
                    StoragePlannedQuery plannedQuery = planner.planQuery((StorageValidatedQuery) validatedQuery);
                    plan.append(plannedQuery.getExecutionWorkflow());
                }
                result = CommandResult.createCommandResult(plan.toString());
            } catch (ParsingException | IgnoreQueryException | ValidationException | PlanningException e) {
                result = Result.createErrorResult(e);
            }
        } else {
            result = Result.createUnsupportedOperationErrorResult(
                    "Invalid number of parameters invoking explain plan");
        }
        return result;
    }

}
