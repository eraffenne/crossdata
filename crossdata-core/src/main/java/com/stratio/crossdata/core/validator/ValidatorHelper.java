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

package com.stratio.crossdata.core.validator;

import com.stratio.crossdata.common.data.*;
import com.stratio.crossdata.common.exceptions.IgnoreQueryException;
import com.stratio.crossdata.common.exceptions.ValidationException;
import com.stratio.crossdata.common.exceptions.validation.*;
import com.stratio.crossdata.common.manifest.PropertyType;
import com.stratio.crossdata.common.metadata.*;
import com.stratio.crossdata.common.statements.structures.*;
import com.stratio.crossdata.core.metadata.MetadataManager;
import com.stratio.crossdata.core.normalizer.Normalizator;
import com.stratio.crossdata.core.query.BaseQuery;
import com.stratio.crossdata.core.query.IParsedQuery;
import com.stratio.crossdata.core.query.SelectParsedQuery;
import com.stratio.crossdata.core.query.SelectValidatedQuery;
import com.stratio.crossdata.core.statements.*;
import com.stratio.crossdata.core.validator.requirements.ValidationTypes;
import org.apache.log4j.Logger;

import java.util.*;

/**
 * Encapsulates the Validation Logic.
 */
public class ValidatorHelper {

    static final int MAX_PRIORITY = 1;
    static final int MIN_PRIORITY = 9;

    private Validator validator;

    private Normalizator normalizator;

    /**
     * Class constructor.
     *
     * @param validator validator instance to be used in validateInsertTypes method.
     */
    public ValidatorHelper(Validator validator){
        this.validator = validator;
    }

    private static final Logger LOG = Logger.getLogger(Validator.class);

    /**
     * @see ValidationTypes#MUST_BE_A_VALID_PK.
     * @param statement
     * @throws NotValidColumnException
     */
    public void validatePKColumn(CrossdataStatement statement) throws NotValidColumnException {

        CreateTableStatement createStm = (CreateTableStatement)  statement;
        createStm.getColumnsWithTypes();
        for (ColumnName pkColumn : createStm.getPrimaryKey()){
            if (!createStm.getColumnsWithTypes().keySet().contains(pkColumn)){
                throw new NotValidColumnException(pkColumn);
            }
        }
    }

    /**
     * @see ValidationTypes#MUST_NOT_EXIST_FULL_TEXT_INDEX.
     * @param statement
     * @throws ExistLuceneIndexException
     */
    public void validateNotExistLuceneIndex(CrossdataStatement statement) throws ExistLuceneIndexException {

        TableName tableName = ((CreateIndexStatement) statement).getTableName();
        TableMetadata tableMetadata = MetadataManager.MANAGER.getTable(tableName);
        Map<IndexName, IndexMetadata> indexes = tableMetadata.getIndexes();

        for (Map.Entry<IndexName, IndexMetadata> entry : indexes.entrySet()) {
            if (entry.getValue().getType() == IndexType.FULL_TEXT) {
                throw new ExistLuceneIndexException(tableName);
            }
        }
    }


    /**
     * @see ValidationTypes#PAGINATION_SUPPORT.
     * @param crossdataStatement
     * @throws BadFormatException
     */
    public void validatePaginationSupport(CrossdataStatement crossdataStatement) throws BadFormatException {
        AttachConnectorStatement acs = (AttachConnectorStatement) crossdataStatement;
        int pageSize = acs.getPagination();
        if (pageSize > 0) {
            ConnectorName connectorName = acs.getConnectorName();
            ConnectorMetadata connector = MetadataManager.MANAGER.getConnector(connectorName);
            Set<Operations> supportedOperations = connector.getSupportedOperations();
            if (!supportedOperations.contains(Operations.PAGINATION)) {
                throw new BadFormatException("Pagination is not supported by the connector " + connectorName);
            }
        }
    }

    /**
     * @see ValidationTypes#VALIDATE_PRIORITY.
     * @param statement
     * @throws BadFormatException
     */
    public void validatePriority(CrossdataStatement statement) throws BadFormatException {
        if (statement instanceof AttachConnectorStatement) {
            Integer priority = ((AttachConnectorStatement) statement).getPriority();


            if (priority < MAX_PRIORITY || priority > MIN_PRIORITY) {

                throw new BadFormatException("The priority is out of range: Must be [1-9]");

            }
        }
    }

    /**
     * Checks if the columns used in the query are within the scope of the table affected by the statement
     *
     * @param statement the Crossdata statement
     */
    public void validateScope(CrossdataStatement statement) throws ValidationException {

        if (statement instanceof StorageStatement) {
            StorageStatement storageStatement = (StorageStatement) statement;
            TableName affectedTableName = storageStatement.getTableName();

            for (ColumnName columnName : storageStatement.getColumns()) {
                if (columnName.getTableName() != null) {
                    if (!columnName.getTableName().getName().equals(affectedTableName.getName())) {
                        throw new NotValidTableException(columnName.getTableName());
                    }
                    if (columnName.getTableName().getCatalogName() != null && !columnName.getTableName()
                            .getCatalogName().getName().equals(affectedTableName.getCatalogName().getName())) {
                        throw new NotValidCatalogException(columnName.getTableName().getCatalogName());
                    }
                }
            }
        }

    }

    /**
     * @see ValidationTypes#MUST_BE_UNIQUE_DATASTORE
     * @param statement
     * @throws BadFormatException
     */
    public void validatePreviousAttachment(CrossdataStatement statement) throws BadFormatException {
        AttachClusterStatement attachClusterStatement = (AttachClusterStatement) statement;
        DataStoreName datastoreName = attachClusterStatement.getDatastoreName();
        ClusterName clusterName = attachClusterStatement.getClusterName();

        if (MetadataManager.MANAGER.exists(clusterName)) {
            ClusterMetadata clusterMetadata = MetadataManager.MANAGER.getCluster(clusterName);
            if (!clusterMetadata.getDataStoreRef().equals(datastoreName)) {
                throw new BadFormatException("A cluster can be attached to only one data store.");
            }
        }
    }

    /**
     * @see ValidationTypes#MUST_EXIST_ATTACH_CONNECTOR_CLUSTER
     * @param statement
     * @throws ValidationException
     */
    public void validateConnectorAttachedRefs(CrossdataStatement statement) throws ValidationException {
        if (statement instanceof DetachConnectorStatement) {
            DetachConnectorStatement detachConnectorStatement = (DetachConnectorStatement) statement;
            ConnectorName connectorName = detachConnectorStatement.getConnectorName();
            ClusterMetadata clusterMetadata = MetadataManager.MANAGER
                    .getCluster(detachConnectorStatement.getClusterName());
            Map<ConnectorName, ConnectorAttachedMetadata> refs = clusterMetadata
                    .getConnectorAttachedRefs();
            Iterator it = refs.entrySet().iterator();
            boolean found = false;
            while (it.hasNext()) {
                Map.Entry<ConnectorName, ConnectorMetadata> pairs = (Map.Entry) it.next();
                if (connectorName.equals(pairs.getKey())) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                throw new ConnectionHasNoRefsException("Invalid validation [MUST_EXIST_ATTACH_CONNECTOR_CLUSTER] for " +
                        statement);
            }
        } else {
            throw new ConnectionHasNoRefsException(
                    "Invalid validation [MUST_EXIST_ATTACH_CONNECTOR_CLUSTER] for " + statement);
        }
    }

    /**
     * @see ValidationTypes#MUST_BE_CONNECTED.
     * @param statement
     * @throws ValidationException
     */
    public void validateConnectorConnected(CrossdataStatement statement) throws ValidationException {
        if (statement instanceof AttachConnectorStatement) {
            AttachConnectorStatement attachConnectorStatement = (AttachConnectorStatement) statement;
            ConnectorName connectorName = attachConnectorStatement.getConnectorName();
            if (!MetadataManager.MANAGER.checkConnectorStatus(connectorName, Status.ONLINE)) {
                throw new NotConnectionException("Connector " + connectorName + " is not connected.");
            }
        } else {
            throw new NotConnectionException("Invalid validation [MUST_BE_CONNECTED] for " + statement);
        }
    }

    private void validateName(boolean exist, Name name, boolean hasIfExist)
            throws IgnoreQueryException, NotExistNameException, ExistNameException {
        if (exist) {
            validateExist(name, hasIfExist);
        } else {
            validateNotExist(name, hasIfExist);
        }
    }

    /**
     * @see ValidationTypes#VALIDATE_SELECT
     * @param parsedQuery
     * @param parentsTableNames
     * @throws ValidationException
     */
    public void  validateSelect(IParsedQuery parsedQuery, Set<TableName> parentsTableNames)
            throws ValidationException {
        SelectParsedQuery selectParsedQuery = (SelectParsedQuery) parsedQuery;
        normalizator = new Normalizator(selectParsedQuery);
        normalizator.execute(parentsTableNames);
    }

    /**
     * @see ValidationTypes#MUST_EXIST_CONNECTOR
     *
     * @param stmt
     * @param exist
     * @throws IgnoreQueryException
     * @throws ExistNameException
     * @throws NotExistNameException
     */
    public void validateConnector(CrossdataStatement stmt, boolean exist) throws IgnoreQueryException,
            ExistNameException, NotExistNameException {
        Name name = null;
        boolean hasIfExist = false;

        if (stmt instanceof AttachConnectorStatement) {
            AttachConnectorStatement attachConnectorStatement = (AttachConnectorStatement) stmt;
            name = attachConnectorStatement.getConnectorName();
        }

        if (stmt instanceof DetachConnectorStatement) {
            DetachConnectorStatement detachConnectorStatement = (DetachConnectorStatement) stmt;
            name = detachConnectorStatement.getConnectorName();
        }

        if (stmt instanceof DropConnectorStatement) {
            DropConnectorStatement dropConnectorStatement = (DropConnectorStatement) stmt;
            name = new ConnectorName(dropConnectorStatement.getName());
        }
        validateName(exist, name, hasIfExist);
    }

    /**
     * @see ValidationTypes#MUST_NOT_EXIST_CLUSTER
     * @param stmt
     * @param exist
     * @throws IgnoreQueryException
     * @throws NotExistNameException
     * @throws ExistNameException
     */
    public void validateCluster(CrossdataStatement stmt, boolean exist) throws IgnoreQueryException,
            NotExistNameException, ExistNameException {
        ClusterName clusterName = null;
        boolean hasIfExists = false;

        if (stmt instanceof AlterClusterStatement) {
            clusterName = ((AlterClusterStatement) stmt).getClusterName();
            hasIfExists = ((AlterClusterStatement) stmt).isIfExists();
        } else if (stmt instanceof AttachClusterStatement) {
            clusterName = (((AttachClusterStatement) stmt).getClusterName());
            hasIfExists = ((AttachClusterStatement) stmt).isIfNotExists();
        } else if (stmt instanceof DetachClusterStatement) {
            clusterName = new ClusterName(((DetachClusterStatement) stmt).getClusterName());
        } else if (stmt instanceof AttachConnectorStatement) {
            clusterName = (((AttachConnectorStatement) stmt).getClusterName());
        } else if (stmt instanceof CreateTableStatement) {
            CreateTableStatement createTableStatement = (CreateTableStatement) stmt;
            clusterName = createTableStatement.getClusterName();
            hasIfExists = createTableStatement.isIfNotExists();
        } else if (stmt instanceof DetachConnectorStatement) {
            DetachConnectorStatement detachConnectorStatement = (DetachConnectorStatement) stmt;
            clusterName = detachConnectorStatement.getClusterName();
        } else if (stmt instanceof ImportMetadataStatement) {
            ImportMetadataStatement importMetadataStatement = (ImportMetadataStatement) stmt;
            clusterName = importMetadataStatement.getClusterName();
        }

        validateName(exist, clusterName, hasIfExists);
    }

    private void validateClusterProperties(DataStoreName name, Map<Selector, Selector> opts)
            throws ValidationException {
        if (!MetadataManager.MANAGER.exists(name)) {
            throw new NotExistNameException(name);
        }
        DataStoreMetadata datastore = MetadataManager.MANAGER.getDataStore(name);
        validateAttachmentProperties(opts, datastore.getRequiredProperties(), datastore.getOthersProperties());
    }

    private void validateConnectorProperties(ConnectorName name, Map<Selector, Selector> opts)
            throws ValidationException {
        if (!MetadataManager.MANAGER.exists(name)) {
            throw new NotExistNameException(name);
        }
        ConnectorMetadata connector = MetadataManager.MANAGER.getConnector(name);
        validateAttachmentProperties(opts, connector.getRequiredProperties(), connector.getOptionalProperties());
    }

    private void validateAttachmentProperties(
            Map<Selector, Selector> opts,
            Set<PropertyType> requiredProps,
            Set<PropertyType> optProps)
            throws ValidationException {

        // Get property names of the attachment
        Set<String> attProps = new HashSet<>();
        for (Selector sel : opts.keySet()) {
            attProps.add(sel.getStringValue());
        }

        // Verify required properties
        Set<String> props = new HashSet<>();
        for (PropertyType pt : requiredProps) {
            props.add(pt.getPropertyName());
        }
        if (!attProps.containsAll(props)) {
            throw new BadFormatException("Some required properties are missing");
        }
        attProps.removeAll(props);

        // Verify optional properties
        props = new HashSet<>();
        for (PropertyType pt : optProps) {
            props.add(pt.getPropertyName());
        }
        if (!props.containsAll(attProps)) {
            throw new BadFormatException("Some properties are not found in the manifest");
        }
    }

    private void validateExist(Name name, boolean hasIfExists)
            throws NotExistNameException, IgnoreQueryException {
        if (!MetadataManager.MANAGER.exists(name)) {
            if (hasIfExists) {
                throw new IgnoreQueryException("[" + name + "] doesn't exist");
            } else {
                throw new NotExistNameException(name);
            }
        }
    }

    private void validateNotExist(Name name, boolean hasIfExists)
            throws ExistNameException, IgnoreQueryException {
        if (MetadataManager.MANAGER.exists(name)) {
            if (hasIfExists) {
                throw new IgnoreQueryException("[" + name + "] doesn't exist");
            } else {
                throw new ExistNameException(name);
            }
        }
    }

    /**
     * @see ValidationTypes#MUST_EXIST_DATASTORE
     *
     * @param statement
     * @param exist
     * @throws IgnoreQueryException
     * @throws NotExistNameException
     * @throws ExistNameException
     */
    public void validateDatastore(CrossdataStatement statement, boolean exist) throws IgnoreQueryException,
            NotExistNameException, ExistNameException {
        Name name = null;
        boolean hasIfExists = false;
        if (statement instanceof AttachClusterStatement) {
            name = (((AttachClusterStatement) statement).getDatastoreName());
            hasIfExists = ((AttachClusterStatement) statement).isIfNotExists();
        } else if (statement instanceof DropDataStoreStatement) {
            name = new DataStoreName(((DropDataStoreStatement) statement).getName());
        }
        validateName(exist, name, hasIfExists);
    }

    /**
     * @see ValidationTypes#MUST_EXIST_COLUMN.
     * @param stmt
     * @param exist
     * @throws NotExistNameException
     * @throws IgnoreQueryException
     * @throws ExistNameException
     */
    public void validateColumn(CrossdataStatement stmt, boolean exist)
            throws NotExistNameException, IgnoreQueryException, ExistNameException {
        ColumnName columnName;
        if (stmt instanceof AlterTableStatement) {
            columnName = ((AlterTableStatement) stmt).getColumn();
            validateName(exist, columnName, false);
        } else if (stmt instanceof CreateIndexStatement) {
            CreateIndexStatement createIndexStatement = (CreateIndexStatement) stmt;
            for (ColumnName column: createIndexStatement.getTargetColumns()) {
                validateName(exist, column, false);
            }
        } else if(stmt instanceof InsertBatchStatement){
            InsertBatchStatement insertBatchStatement = (InsertBatchStatement) stmt;
            for(ColumnName column: insertBatchStatement.getColumnNames()){
                validateName(exist, column, false);
            }
        }
    }

    /**
     * @see ValidationTypes#MUST_EXIST_PROPERTIES.
     * @param stmt
     * @throws ValidationException
     */
    public void validateExistsProperties(CrossdataStatement stmt) throws ValidationException {

        if (stmt instanceof AlterCatalogStatement) {
            AlterCatalogStatement alterCatalogStatement = (AlterCatalogStatement) stmt;
            if (alterCatalogStatement.getOptions() == null || alterCatalogStatement.getOptions().isEmpty()) {
                throw new BadFormatException("AlterCatalog options can't be empty");
            }

        } else if (stmt instanceof AlterClusterStatement) {
            AlterClusterStatement alterClusterStatement = (AlterClusterStatement) stmt;
            if (alterClusterStatement.getOptions() == null || alterClusterStatement.getOptions().isEmpty()) {
                throw new BadFormatException("AlterCluster options can't be empty");
            }
        }

    }

    /**
     * @see ValidationTypes#MUST_EXIST_TABLE .
     *
     * @param stmt
     * @param exist
     * @throws NotExistNameException
     * @throws IgnoreQueryException
     * @throws ExistNameException
     */
    public void validateTable(CrossdataStatement stmt, boolean exist)
            throws NotExistNameException, IgnoreQueryException, ExistNameException {
        TableName tableName;
        boolean hasIfExists = false;

        if (stmt instanceof AlterTableStatement) {
            tableName = ((AlterTableStatement) stmt).getTableName();
        } else if (stmt instanceof DropTableStatement) {
            tableName = ((DropTableStatement) stmt).getTableName();
            hasIfExists = ((DropTableStatement) stmt).isIfExists();
        } else if (stmt instanceof CreateTableStatement) {
            CreateTableStatement createTableStatement = (CreateTableStatement) stmt;
            tableName = createTableStatement.getTableName();
            hasIfExists = createTableStatement.isIfNotExists();
        } else if (stmt instanceof InsertIntoStatement) {
            InsertIntoStatement insertIntoStatement = (InsertIntoStatement) stmt;
            tableName = insertIntoStatement.getTableName();
        } else if (stmt instanceof DeleteStatement) {
            DeleteStatement deleteStatement = (DeleteStatement) stmt;
            tableName = deleteStatement.getTableName();
        } else if (stmt instanceof DetachClusterStatement) {
            DetachClusterStatement detachClusterStatement = (DetachClusterStatement) stmt;
            tableName = detachClusterStatement.getTableMetadata().getName();
        } else if (stmt instanceof AttachClusterStatement) {
            AttachClusterStatement attachClusterStatement = (AttachClusterStatement) stmt;
            tableName = attachClusterStatement.getTableMetadata().getName();
            hasIfExists = attachClusterStatement.isIfNotExists();
        } else if (stmt instanceof CreateIndexStatement) {
            CreateIndexStatement createIndexStatement = (CreateIndexStatement) stmt;
            tableName = createIndexStatement.getTableName();
            hasIfExists = createIndexStatement.isCreateIfNotExists();
        } else if (stmt instanceof DropIndexStatement) {
            DropIndexStatement dropIndexStatement = (DropIndexStatement) stmt;
            tableName = dropIndexStatement.getName().getTableName();
        } else if (stmt instanceof UpdateTableStatement) {
            UpdateTableStatement updateTableStatement = (UpdateTableStatement) stmt;
            tableName = updateTableStatement.getTableName();
        } else if (stmt instanceof TruncateStatement) {
            TruncateStatement truncateStatement = (TruncateStatement) stmt;
            tableName = truncateStatement.getTableName();
        } else if (stmt instanceof ImportMetadataStatement) {
            ImportMetadataStatement importMetadataStatement = (ImportMetadataStatement) stmt;
            tableName = importMetadataStatement.getTableName();
        } else if (stmt instanceof InsertBatchStatement) {
            InsertBatchStatement insertBatchStatement = (InsertBatchStatement) stmt;
            tableName = insertBatchStatement.getTableName();
        } else {
            throw new IgnoreQueryException(stmt.getClass().getCanonicalName() + " not supported yet.");
        }

        validateName(exist, tableName, hasIfExists);
    }

    /**
     * @see ValidationTypes#MUST_NOT_EXIST_CATALOG#validate().
     *
     * @param stmt
     * @param exist
     * @throws IgnoreQueryException
     * @throws ExistNameException
     * @throws NotExistNameException
     */
    public void validateCatalog(CrossdataStatement stmt, boolean exist)
            throws IgnoreQueryException, ExistNameException, NotExistNameException {
        CatalogName catalogName = null;
        boolean validate = true;
        boolean hasIfExists = false;
        if (stmt instanceof AlterCatalogStatement) {
            AlterCatalogStatement alterCatalogStatement = (AlterCatalogStatement) stmt;
            catalogName = alterCatalogStatement.getCatalogName();
        } else if (stmt instanceof CreateCatalogStatement) {
            CreateCatalogStatement createCatalogStatement = (CreateCatalogStatement) stmt;
            hasIfExists = createCatalogStatement.isIfNotExists();
            catalogName = createCatalogStatement.getCatalogName();
        } else if (stmt instanceof DropCatalogStatement) {
            DropCatalogStatement dropCatalogStatement = (DropCatalogStatement) stmt;
            hasIfExists = dropCatalogStatement.isIfExists();
            catalogName = dropCatalogStatement.getCatalogName();
        } else if (stmt instanceof CreateTableStatement) {
            CreateTableStatement createTableStatement = (CreateTableStatement) stmt;
            catalogName = createTableStatement.getEffectiveCatalog();
            hasIfExists = createTableStatement.isIfNotExists();
        } else if (stmt instanceof DropTableStatement) {
            DropTableStatement dropTableStatement = (DropTableStatement) stmt;
            catalogName = dropTableStatement.getCatalogName();
            hasIfExists = dropTableStatement.isIfExists();
        } else if (stmt instanceof InsertIntoStatement) {
            InsertIntoStatement insertIntoStatement = (InsertIntoStatement) stmt;
            catalogName = insertIntoStatement.getCatalogName();
        } else if (stmt instanceof ImportMetadataStatement) {
            ImportMetadataStatement importMetadataStatement = (ImportMetadataStatement) stmt;
            catalogName = importMetadataStatement.getCatalogName();
        } else if (stmt instanceof CreateIndexStatement) {
            CreateIndexStatement createIndexStatement = (CreateIndexStatement) stmt;
            catalogName = createIndexStatement.getTableName().getCatalogName();
        } else if (stmt instanceof DropIndexStatement) {
            DropIndexStatement dropIndexStatement = (DropIndexStatement) stmt;
            catalogName = dropIndexStatement.getName().getTableName().getCatalogName();
        } else {
            //TODO: should through exception?
            //Correctness - Method call passes null for notnull parameter
            validate = false;
        }

        if (validate) {
            validateName(exist, catalogName, hasIfExists);
        }
    }

    /**
     * @see ValidationTypes#VALID_CLUSTER_OPTIONS.
     *
     * @param stmt
     * @throws ValidationException
     */
    public void validateOptions(CrossdataStatement stmt) throws ValidationException {
        if (stmt instanceof AttachClusterStatement) {
            AttachClusterStatement myStmt = (AttachClusterStatement) stmt;
            validateClusterProperties(myStmt.getDatastoreName(), myStmt.getOptions());
        } else if (stmt instanceof AttachConnectorStatement) {
            AttachConnectorStatement myStmt = (AttachConnectorStatement) stmt;
            validateConnectorProperties(myStmt.getConnectorName(), myStmt.getOptions());
        }
    }

    /**
     * @see ValidationTypes#MUST_NOT_EXIST_INDEX.
     * @param stmt
     * @param exist
     * @throws IgnoreQueryException
     * @throws ExistNameException
     * @throws NotExistNameException
     */
    public void validateIndex(CrossdataStatement stmt, boolean exist)
            throws IgnoreQueryException, ExistNameException, NotExistNameException {
        Name name = null;
        boolean hasIfExist = false;
        if (stmt instanceof CreateIndexStatement) {
            CreateIndexStatement createIndexStatement = (CreateIndexStatement) stmt;
            name = createIndexStatement.getName();
            hasIfExist = createIndexStatement.isCreateIfNotExists();
        }

        if (stmt instanceof DropIndexStatement) {
            DropIndexStatement dropIndexStatement = (DropIndexStatement) stmt;
            name = dropIndexStatement.getName();
            hasIfExist = dropIndexStatement.isDropIfExists();
        }
        validateName(exist, name, hasIfExist);
    }


    private void checkValuesLength(int idsLength, int valuesLength) throws BadFormatException {
        if (idsLength != valuesLength) {
            throw new BadFormatException(
                    "Values length doesn't correspond to the identifiers length" +
                            System.lineSeparator() +
                            "Identifiers Length = " + idsLength +
                            System.lineSeparator() +
                            "Values Length = " + valuesLength);
        }
    }

    private void validateColumnType(ColumnMetadata columnMetadata, ColumnMetadata columnFromSelect)
            throws NotMatchDataTypeException, BadFormatException {
        Selector selector = columnFromSelect.getColumnType().createSelector();
        validateColumnType(columnMetadata, selector, false);
    }

    private Selector validateColumnType(ColumnMetadata columnMetadata, Selector querySelector, boolean tryConversion)
            throws BadFormatException, NotMatchDataTypeException {
        NotMatchDataTypeException notMatchDataTypeException = null;
        BadFormatException badFormatException = null;
        Selector resultingSelector = querySelector;
        switch (querySelector.getType()) {
            case FUNCTION:
                LOG.info("Functions are not supported yet for this statement.");
                break;
            case COLUMN:
                ColumnName queryColumnName = ((ColumnSelector) querySelector).getName();
                ColumnMetadata rightColumnMetadata = MetadataManager.MANAGER.getColumn(queryColumnName);
                if (columnMetadata.getColumnType() != rightColumnMetadata.getColumnType()) {
                    notMatchDataTypeException = new NotMatchDataTypeException(queryColumnName);
                }
                break;
            case ASTERISK:
                badFormatException = new BadFormatException("Asterisk not supported in relations.");
                break;
            case BOOLEAN:
                if (columnMetadata.getColumnType().getDataType() != DataType.BOOLEAN) {
                    notMatchDataTypeException = new NotMatchDataTypeException(columnMetadata.getName());
                }
                break;
            case STRING:
                if ((columnMetadata.getColumnType().getDataType() != DataType.TEXT) &&
                        (columnMetadata.getColumnType().getDataType() != DataType.NATIVE)) {
                    notMatchDataTypeException = new NotMatchDataTypeException(columnMetadata.getName());
                }
                break;
            case INTEGER:
                if ((columnMetadata.getColumnType().getDataType() != DataType.INT) &&
                        (columnMetadata.getColumnType().getDataType() != DataType.BIGINT)) {
                    if (tryConversion) {
                        resultingSelector = convertIntegerSelector(
                                (IntegerSelector) querySelector,
                                columnMetadata.getColumnType(),
                                columnMetadata.getName());
                    } else {
                        notMatchDataTypeException = new NotMatchDataTypeException(columnMetadata.getName());
                    }
                }
                break;
            case FLOATING_POINT:
                if ((columnMetadata.getColumnType().getDataType() != DataType.FLOAT) &&
                        (columnMetadata.getColumnType().getDataType() != DataType.DOUBLE)) {
                    notMatchDataTypeException = new NotMatchDataTypeException(columnMetadata.getName());
                }
                break;
            case RELATION:
                badFormatException = new BadFormatException("Operation not supported in where.");
                break;
            default:
                break;
        }

        if (notMatchDataTypeException != null) {
            throw notMatchDataTypeException;
        } else if (badFormatException != null) {
            throw badFormatException;
        }
        return resultingSelector;
    }

    private Selector convertIntegerSelector(IntegerSelector querySelector, ColumnType columnType, ColumnName name)
            throws NotMatchDataTypeException {
        Selector resultingSelector;
        if (columnType.getDataType() == DataType.DOUBLE || columnType.getDataType() == DataType.FLOAT) {
            resultingSelector = new FloatingPointSelector(querySelector.getTableName(), querySelector.getValue());
        } else {
            throw new NotMatchDataTypeException(name);
        }
        return resultingSelector;
    }

    /**
     * @see ValidationTypes#VALIDATE_TYPES
     * @param parsedQuery
     * @throws ValidationException
     * @throws IgnoreQueryException
     */
    public void validateInsertTypes(IParsedQuery parsedQuery)
            throws ValidationException, IgnoreQueryException {
        CrossdataStatement stmt = parsedQuery.getStatement();
        if (stmt instanceof InsertIntoStatement) {
            InsertIntoStatement insertIntoStatement = (InsertIntoStatement) stmt;
            List<ColumnName> columnNameList = insertIntoStatement.getColumns();

            if (insertIntoStatement.getTypeValues() == InsertIntoStatement.TYPE_VALUES_CLAUSE) {
                List<Selector> selectorList = insertIntoStatement.getCellValues();
                List<Selector> resultingList = new ArrayList<>();
                for (int i = 0; i < columnNameList.size(); i++) {
                    ColumnName columnName = columnNameList.get(i);
                    Selector valueSelector = selectorList.get(i);
                    ColumnMetadata columnMetadata = MetadataManager.MANAGER.getColumn(columnName);
                    Selector resultingSelector = validateColumnType(columnMetadata, valueSelector, true);
                    resultingList.add(resultingSelector);
                }
                insertIntoStatement.setCellValues(resultingList);
            } else if (insertIntoStatement.getTypeValues() == InsertIntoStatement.TYPE_SELECT_CLAUSE) {
                SelectStatement ss = insertIntoStatement.getSelectStatement();

                List<ColumnName> cols = ss.getColumns();

                List<Selector> selectorList = ss.getSelectExpression().getSelectorList();
                if ((selectorList.size()) == 1 && (selectorList.get(0) instanceof AsteriskSelector)) {
                    cols.clear();
                    List<ColumnMetadata> columnsMetadata = MetadataManager.MANAGER.getColumnByTable(
                            ss.getTableName().getCatalogName().getName(),
                            ss.getTableName().getName());
                    for (ColumnMetadata cm : columnsMetadata) {
                        cols.add(cm.getName());
                    }
                }

                checkValuesLength(columnNameList.size(), cols.size());

                BaseQuery baseQuery = new BaseQuery(
                        UUID.randomUUID().toString(),
                        ss.toString(),
                        parsedQuery.getDefaultCatalog(),parsedQuery.getSessionId());

                SelectParsedQuery spq = new SelectParsedQuery(baseQuery, ss);
                SelectValidatedQuery selectValidatedQuery = (SelectValidatedQuery) validator.validate(spq);

                cols = selectValidatedQuery.getColumns();

                for (int i = 0; i < columnNameList.size(); i++) {
                    ColumnName columnName = columnNameList.get(i);
                    ColumnMetadata columnFromSelect = MetadataManager.MANAGER.getColumn(cols.get(i));
                    ColumnMetadata columnMetadata = MetadataManager.MANAGER.getColumn(columnName);
                    validateColumnType(columnMetadata, columnFromSelect);
                }

                insertIntoStatement.setSelectStatement(selectValidatedQuery.getStatement());
            }

        }
    }

    public Normalizator getNormalizator() {
        return normalizator;
    }

}
