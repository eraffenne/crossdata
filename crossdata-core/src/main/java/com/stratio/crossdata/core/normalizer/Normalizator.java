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

package com.stratio.crossdata.core.normalizer;

import static com.stratio.crossdata.common.statements.structures.SelectorType.COLUMN;
import static com.stratio.crossdata.common.statements.structures.SelectorType.FUNCTION;
import static com.stratio.crossdata.common.statements.structures.SelectorType.RELATION;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;

import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.data.IndexName;
import com.stratio.crossdata.common.data.TableName;
import com.stratio.crossdata.common.exceptions.IgnoreQueryException;
import com.stratio.crossdata.common.exceptions.ValidationException;
import com.stratio.crossdata.common.exceptions.validation.AmbiguousNameException;
import com.stratio.crossdata.common.exceptions.validation.BadFormatException;
import com.stratio.crossdata.common.exceptions.validation.NotExistNameException;
import com.stratio.crossdata.common.exceptions.validation.NotMatchDataTypeException;
import com.stratio.crossdata.common.exceptions.validation.NotValidCatalogException;
import com.stratio.crossdata.common.exceptions.validation.NotValidColumnException;
import com.stratio.crossdata.common.exceptions.validation.NotValidTableException;
import com.stratio.crossdata.common.exceptions.validation.YodaConditionException;
import com.stratio.crossdata.common.metadata.ColumnMetadata;
import com.stratio.crossdata.common.metadata.ColumnType;
import com.stratio.crossdata.common.metadata.DataType;
import com.stratio.crossdata.common.metadata.IndexMetadata;
import com.stratio.crossdata.common.metadata.IndexType;
import com.stratio.crossdata.common.metadata.TableMetadata;
import com.stratio.crossdata.common.statements.structures.AbstractRelation;
import com.stratio.crossdata.common.statements.structures.AliasSelector;
import com.stratio.crossdata.common.statements.structures.CaseWhenSelector;
import com.stratio.crossdata.common.statements.structures.ColumnSelector;
import com.stratio.crossdata.common.statements.structures.FunctionSelector;
import com.stratio.crossdata.common.statements.structures.GroupSelector;
import com.stratio.crossdata.common.statements.structures.Operator;
import com.stratio.crossdata.common.statements.structures.OrderByClause;
import com.stratio.crossdata.common.statements.structures.Relation;
import com.stratio.crossdata.common.statements.structures.RelationDisjunction;
import com.stratio.crossdata.common.statements.structures.RelationSelector;
import com.stratio.crossdata.common.statements.structures.RelationTerm;
import com.stratio.crossdata.common.statements.structures.SelectExpression;
import com.stratio.crossdata.common.statements.structures.Selector;
import com.stratio.crossdata.common.statements.structures.SelectorType;
import com.stratio.crossdata.common.utils.Constants;
import com.stratio.crossdata.core.metadata.MetadataManager;
import com.stratio.crossdata.core.query.SelectParsedQuery;
import com.stratio.crossdata.core.query.SelectValidatedQuery;
import com.stratio.crossdata.core.statements.SelectStatement;
import com.stratio.crossdata.core.structures.ExtendedSelectSelector;
import com.stratio.crossdata.core.structures.GroupByClause;
import com.stratio.crossdata.core.structures.Join;
import com.stratio.crossdata.core.validator.Validator;

/**
 * Normalizator Class.
 */
public class Normalizator {

    /**
     * Class logger.
     */
    private static final Logger LOG = Logger.getLogger(Normalizator.class);

    private NormalizedFields fields = new NormalizedFields();

    private SelectParsedQuery parsedQuery;

    private Normalizator subqueryNormalizator;

    private Map<String, Integer> aliasFunctionMap = new HashMap<>();

    private Set<TableName> joinTableSet = new HashSet<>();

    private Set<String> aliasSet =new HashSet<>();

    /**
     * Class Constructor.
     *
     * @param parsedQuery The parsed query.
     */
    public Normalizator(SelectParsedQuery parsedQuery) {
        this.parsedQuery = parsedQuery;
    }

    /**
     * Get the parsed query to Normalize.
     *
     * @return com.stratio.crossdata.core.query.SelectParsedQuery;
     */
    public SelectParsedQuery getParsedQuery() {
        return parsedQuery;
    }

    /**
     * Get the Normalizator of the subquery.
     *
     * @return com.stratio.crossdata.core.normalizer.Normalizator;
     */
    public Normalizator getSubqueryNormalizator() {
        return subqueryNormalizator;
    }

    /**
     * Get the obtained fields normalized.
     *
     * @return com.stratio.crossdata.core.normalizer.NormalizerFields
     */
    public NormalizedFields getFields() {
        return fields;
    }

    /**
     * Execute the normalization of a parsed query.
     * @param parentsTableNames A set of {@link com.stratio.crossdata.common.data.TableName}.
     * @throws ValidationException .
     */
    public void execute(Set<TableName> parentsTableNames) throws ValidationException {

        normalizeTables();

        fields.getTableNames().addAll(parentsTableNames);
        fields.setPreferredTableNames(new HashSet<>(parsedQuery.getStatement().getFromTables()));

        if (parsedQuery.getStatement().isSubqueryInc()) {

            Set<TableName> previousPreferredTablesFromFields = new HashSet<>(fields.getPreferredTableNames());

            subqueryNormalizator = new Normalizator(parsedQuery.getChildParsedQuery());
            subqueryNormalizator.execute(fields.getTableNames());
            checkSubquerySelectors(subqueryNormalizator.getFields().getSelectors());

            fields.getPreferredTableNames().clear();
            fields.getPreferredTableNames().addAll(previousPreferredTablesFromFields);

        }

        normalizeSelectExpression();
        normalizeWhere();
        normalizeJoins();
        normalizeOrderBy();
        normalizeGroupBy();
        normalizeHaving();
        validateColumnsScope();
    }

    private void checkSubquerySelectors(List<Selector> selectors) throws ValidationException {
        Set<String> uniqueSelectors = new HashSet<>();

        for (Selector selector : selectors) {
            if (selector.getAlias() != null) {
                if (!uniqueSelectors.add(selector.getAlias())) {
                    throw new AmbiguousNameException("The alias [" + selector.getAlias() + "] is duplicate");
                }
            } else if (selector instanceof ColumnSelector) {
                if (!uniqueSelectors.add(selector.getColumnName().getName())) {
                    throw new AmbiguousNameException(
                            "The selector [" + selector.getColumnName().getName() + "] is ambiguous. Try using alias");
                }
            } else {
                if (!uniqueSelectors.add(selector.getStringValue())) {
                    throw new AmbiguousNameException(
                            "The selector [" + selector.getStringValue() + "] is duplicate. Try using alias");
                }
            }
        }
    }

    /**
     * Normalize the tables of a query.
     *
     * @throws ValidationException
     */
    public void normalizeTables() throws ValidationException {
        List<TableName> tableNames = parsedQuery.getStatement().getFromTables();
        if (tableNames != null && !tableNames.isEmpty()) {
            normalizeTables(tableNames);
        }
    }

    /**
     * Normalize the "from" tables of a parsed query.
     *
     * @param fromTables List of the from clause tables of a parsed query
     * @throws ValidationException
     */
    public void normalizeTables(List<TableName> fromTables) throws ValidationException {
        for (TableName tableName : fromTables) {
            if (!tableName.isVirtual()) {
                checkTable(tableName);
            }
            fields.getCatalogNames().add(tableName.getCatalogName());
            fields.addTableName(tableName);
        }
    }

    /**
     * Normalize all the joins of a parsed query.
     *
     * @throws ValidationException
     */
    public void normalizeJoins() throws ValidationException {
        List<Join> joinList = parsedQuery.getStatement().getJoinList();
        if (!joinList.isEmpty()) {
            addImplicitJoins(joinList);
            for (Join join : joinList) {
                normalizeJoins(join);
                fields.addJoin(join);
            }
        }
    }

    private void addImplicitJoins(List<Join> joinList) {
        if (fields.getImplicitWhere() == null || fields.getImplicitWhere().isEmpty()) {
            return;
        }

        for (Join join : joinList) {
            List<TableName> tablesFromJoin = join.getTableNames();
            for (Relation r : fields.getImplicitWhere()) {
                if (tablesFromJoin.contains(r.getLeftTerm().getTableName())
                        || tablesFromJoin.contains(r.getRightTerm().getTableName())) {
                    join.addRelation(r);
                }
            }
        }
    }

    /**
     * Normalize a specific inner join of a parsed query.
     *
     * @param join The inner join
     * @throws ValidationException
     */
    public void normalizeJoins(Join join) throws ValidationException {
        joinTableSet.addAll(join.getTableNames());
        checkJoinRelations(join.getRelations());
    }

    private void normalizeWhere() throws ValidationException {
        List<AbstractRelation> whereClauses = parsedQuery.getStatement().getWhere();
        if ((whereClauses != null) && (!whereClauses.isEmpty())) {
            normalizeWhere(whereClauses);
            fields.setWhere(whereClauses);
        }
    }

    private void normalizeWhere(List<AbstractRelation> where) throws ValidationException {
        checkWhereRelations(where);
    }

    /**
     * Normalize the order by clause of a parsed query.
     *
     * @throws ValidationException
     */
    public void normalizeOrderBy() throws ValidationException {
        List<OrderByClause> orderByClauseClauses = parsedQuery.getStatement().getOrderByClauses();
        if (orderByClauseClauses != null) {
            normalizeOrderBy(orderByClauseClauses);
            fields.setOrderByClauses(orderByClauseClauses);
        }
    }

    /**
     * Normalize an specific order by of a parsed query.
     *
     * @param orderByClauseClauses The order by
     * @throws ValidationException
     */
    public void normalizeOrderBy(List<OrderByClause> orderByClauseClauses) throws ValidationException {
        for (OrderByClause orderBy : orderByClauseClauses) {
            Selector selector = orderBy.getSelector();
            switch (selector.getType()) {
            case COLUMN:
                Selector referencedSelector = findReferencedSelector(((ColumnSelector) selector).getName().getName());
                if (referencedSelector != null) {
                    orderBy.setSelector(new AliasSelector(referencedSelector));
                } else {
                    checkColumnSelector((ColumnSelector) selector);
                }
                break;
            case FUNCTION:
                FunctionSelector fs = (FunctionSelector) selector;
                List<Selector> fCols = fs.getFunctionColumns();
                checkListSelector(fCols);
                break;
            case ASTERISK:
            case BOOLEAN:
            case STRING:
            case INTEGER:
            case FLOATING_POINT:
                throw new BadFormatException("Order by only accepts columns");
            }
        }
    }

    /**
     * Normalize de select clause of a parsed query.
     *
     * @throws ValidationException
     */
    public void normalizeSelectExpression() throws ValidationException {
        SelectExpression selectExpression = parsedQuery.getStatement().getSelectExpression();
        if (selectExpression != null) {
            normalizeSelectExpression(selectExpression);
        }
    }

    /**
     * Normalize an specific select expression.
     *
     * @param selectExpression The select expression
     * @throws ValidationException
     */
    public void normalizeSelectExpression(
            SelectExpression selectExpression) throws ValidationException {
        List<Selector> normalizeSelectors = checkListSelector(selectExpression.getSelectorList());
        fields.getSelectors().addAll(normalizeSelectors);
        selectExpression.getSelectorList().clear();
        selectExpression.getSelectorList().addAll(normalizeSelectors);
    }

    /**
     * Normalize the group by of a parsed query.
     *
     * @throws ValidationException
     */
    public void normalizeGroupBy() throws ValidationException {
        //Ensure that there is not any case-when selector
        for (Selector s : fields.getSelectors()) {
            if (CaseWhenSelector.class.isInstance(s)) {
                throw new BadFormatException("Group By clause is not possible with Case When Selector");
            }
        }

        GroupByClause groupByClause = parsedQuery.getStatement().getGroupByClause();
        if (groupByClause != null) {
            normalizeGroupBy(groupByClause);
            fields.setGroupByClause(groupByClause);
        }
    }

    /**
     * Normalize an specific group by of a parsed query.
     *
     * @param groupByClause
     * @throws ValidationException
     */
    public void normalizeGroupBy(GroupByClause groupByClause) throws ValidationException {
        Set<ColumnName> columnNamesFromGroupBy = checkGBFormatBySelectorIdentifier(
                groupByClause.getSelectorIdentifier());

        // Check if all columns are correct
        for (Selector selector : fields.getSelectors()) {
            checkGroupByColumns(selector, columnNamesFromGroupBy);
        }
    }

    private Set<ColumnName> checkGBFormatBySelectorIdentifier(List<Selector> selectorList)
            throws ValidationException {
        Set<ColumnName> columnNames = new LinkedHashSet<>();
        int selectorIndex = 0;

        for (Selector selector : selectorList) {
            switch (selector.getType()) {
            case FUNCTION:
                checkFunctionSelector((FunctionSelector) selector);
                break;
            case COLUMN:
                Selector referencedSelector = findReferencedSelector(((ColumnSelector) selector).getName().getName());
                if (referencedSelector != null) {
                    selectorList.set(selectorIndex, new AliasSelector(referencedSelector));
                } else {
                    checkColumnSelector((ColumnSelector) selector);
                    if (!columnNames.add(((ColumnSelector) selector).getName())) {
                        throw new BadFormatException("COLUMN into group by is repeated");
                    }
                }
                break;
            case ASTERISK:
                throw new BadFormatException("Asterisk include into groupBy is not valid");
            }
            selectorIndex++;
        }
        return columnNames;
    }

    private Selector findReferencedSelector(String selectorAlias) {
        Selector referencedSelector = null;
        boolean aliasFound = false;
        Iterator<Selector> normalizedSelectExpressionIterator = parsedQuery.getStatement().getSelectExpression()
                .getSelectorList().iterator();

        while (normalizedSelectExpressionIterator.hasNext() && !aliasFound) {
            referencedSelector = normalizedSelectExpressionIterator.next();
            if (referencedSelector.getType() != SelectorType.COLUMN && referencedSelector.getAlias() != null
                    && referencedSelector.getAlias().equals(selectorAlias)) {
                aliasFound = true;
            }
        }
        return aliasFound ? referencedSelector : null;
    }

    /**
     * Normalize the having of a parsed query.
     *
     * @throws ValidationException
     */
    public void normalizeHaving() throws ValidationException {
        List<AbstractRelation> havingClause = parsedQuery.getStatement().getHavingClause();
        if (havingClause != null) {

            if (!parsedQuery.getStatement().isGroupInc()) {
                throw new BadFormatException("Having clause requires Group By clause.");
            }

            List<AbstractRelation> havingClauses = parsedQuery.getStatement().getHavingClause();
            if ((havingClauses != null) && (!havingClauses.isEmpty())) {
                normalizeHaving(havingClauses);
                fields.setHavingClause(havingClauses);
            }
        }
    }


    private void checkGroupByColumns(Selector selector, Set<ColumnName> columnNames) throws BadFormatException {
        switch (selector.getType()) {
        case FUNCTION:
            break;
        case COLUMN:
            ColumnName name = ((ColumnSelector) selector).getName();
            if (!columnNames.contains(name)) {
                throw new BadFormatException(
                        "All columns in the select clause must be in the group by or it must be aggregation includes.");
            }
            break;
        case ASTERISK:
            throw new BadFormatException("Asterisk is not valid with group by statements");
        }
    }


    /**
     * Normalize an specific Having of a parsed query.
     *
     * @param havingClause
     * @throws ValidationException
     */
    public void normalizeHaving(List<AbstractRelation> havingClause) throws ValidationException {
        for (AbstractRelation relation : havingClause) {
            checkRelation(relation);
        }
    }

    private void validateColumnsScope() throws ValidationException {
        for (ColumnName columnName : fields.getColumnNames()) {
            String expectedTableName = columnName.getTableName().getQualifiedName();
            Iterator<TableName> tableNamesIterator = fields.getTableNames().iterator();
            boolean tableFound = false;
            while (!tableFound && tableNamesIterator.hasNext()) {
                tableFound = tableNamesIterator.next().getQualifiedName().equals(expectedTableName);
            }
            if (!tableFound) {
                throw new NotValidTableException(
                        "The table [" + expectedTableName + "] is not within the scope of the query");
            }
        }
    }

    /**
     * Validate the joins of a parsed query.
     *
     * @param relations A list of Relation to check.
     * @throws ValidationException
     */
    public void checkJoinRelations(List<AbstractRelation> relations) throws ValidationException {
        for (AbstractRelation relation : relations) {
            if (relation instanceof RelationDisjunction) {
                throw new BadFormatException("Join relations cannot contain or operators");
            }

            checkRelation(relation);
            checkJoinRelation(relation);
            checkJoinTablesRelation(relation);
        }
    }

    private void checkJoinTablesRelation(AbstractRelation abstractRelation) throws ValidationException {
        if (abstractRelation instanceof Relation) {
            Relation relation = (Relation) abstractRelation;
            if (!joinTableSet.contains(relation.getLeftTerm().getColumnName().getTableName()) ||
                    !joinTableSet.contains(relation.getRightTerm().getColumnName().getTableName())){
                throw new BadFormatException("Join relation malformed. You can only use columns of tables implied in " +
                        "the actual join and previous joins");
            }
        }
    }

    /**
     * Check if the join relation is correct.
     * @param abstractRelation The relation of the join.
     * @throws ValidationException .
     */
    public void checkJoinRelation(AbstractRelation abstractRelation) throws ValidationException {
        if (abstractRelation instanceof Relation) {
            Relation relation = (Relation) abstractRelation;
            switch (relation.getOperator()) {
            case EQ:
            case GT:
            case LT:
            case GET:
            case LET:
            case DISTINCT:
                if (relation.getLeftTerm().getType() == SelectorType.COLUMN
                        && relation.getRightTerm().getType() == SelectorType.COLUMN) {
                    checkColumnSelector((ColumnSelector) relation.getRightTerm());
                    checkColumnSelector((ColumnSelector) relation.getLeftTerm());
                } else {
                    throw new BadFormatException("You must compare between columns");
                }
                break;
            default:
                throw new BadFormatException("Only equal operation are valid");
            }
        } else if (abstractRelation instanceof RelationDisjunction) {
            RelationDisjunction rd = (RelationDisjunction) abstractRelation;
            for (RelationTerm rt : rd.getTerms()) {
                for (AbstractRelation ar : rt.getRelations()) {
                    checkRelation(ar);
                }
            }
        }
    }

    /**
     * Validate the where clause of a parsed query.
     *
     * @param relations The list of Relation that contains the where clause to check
     * @throws ValidationException
     */
    public void checkWhereRelations(List<AbstractRelation> relations) throws ValidationException {
        Set<Integer> toBeRemoved = new HashSet();
        int count = 0;
        for (AbstractRelation relation : relations) {
            boolean implicit = checkRelation(relation);
            if (implicit) {
                toBeRemoved.add(count);
            }
            count++;
        }
        // Remove where clauses corresponding to common fields of implicit joins
        int nRemoved = 0;
        for (int pos : toBeRemoved) {
            Relation relation = (Relation) relations.remove(pos - nRemoved);
            getFields().addImplicitWhere(relation);
            nRemoved++;
        }
    }

    /**
     * Validate the relation of a parsed query.
     *
     * @param abstractRelation The relation of the query.
     * @throws ValidationException
     * @return Whether the relation implies that is a implicit join.
     */
    public boolean checkRelation(AbstractRelation abstractRelation) throws ValidationException {
        boolean implicit = false;
        if (abstractRelation instanceof Relation) {
            Relation relationConjunction = (Relation) abstractRelation;
            if (relationConjunction.getOperator().isInGroup(Operator.Group.ARITHMETIC)) {
                throw new BadFormatException("Comparing operations are the only valid ones");
            }
            checkRelationFormatLeft(relationConjunction);
            checkRelationFormatRight(relationConjunction);
            implicit = checkImplicitRelation(relationConjunction);
        } else if (abstractRelation instanceof RelationDisjunction) {
            RelationDisjunction rd = (RelationDisjunction) abstractRelation;
            for (RelationTerm rt : rd.getTerms()) {
                for (AbstractRelation ar : rt.getRelations()) {
                    checkRelation(ar);
                }
            }
        }
        return implicit;
    }

    private boolean checkImplicitRelation(Relation relation) {
        boolean implicit = false;
        if ((relation.getLeftTerm() instanceof ColumnSelector) && (relation.getRightTerm() instanceof ColumnSelector)) {
            ColumnSelector leftColumn = (ColumnSelector) relation.getLeftTerm();
            ColumnSelector rightColumn = (ColumnSelector) relation.getRightTerm();
            if (!leftColumn.getTableName().equals(rightColumn.getTableName())) {
                implicit = true;
            }
        }
        return implicit;
    }



    private void checkRelationFormatLeft(Relation relation) throws ValidationException {
        switch (relation.getLeftTerm().getType()) {
        case COLUMN:
            checkColumnSelector((ColumnSelector) relation.getLeftTerm());
            break;
        case ASTERISK:
            throw new BadFormatException("Asterisk not supported in relations.");
        case STRING:
        case FLOATING_POINT:
        case BOOLEAN:
        case INTEGER:
            if (relation.getOperator() == Operator.EQ) {
                throw new YodaConditionException();
            }
            break;
        case SELECT:
            ExtendedSelectSelector extendedSelectSelector = (ExtendedSelectSelector) relation.getLeftTerm();
            SelectValidatedQuery selectValidatedQuery = normalizeInnerSelect(
                    extendedSelectSelector.getSelectParsedQuery(),
                    fields.getTableNames());
            extendedSelectSelector.setSelectValidatedQuery(selectValidatedQuery);
            break;
        case RELATION:
            throw new BadFormatException("Relations can't be on the left side of other relations.");
        case FUNCTION:
            checkFunctionSelector((FunctionSelector) relation.getLeftTerm());
            break;
        default:
            throw new BadFormatException("Relations can't support this selector type: "+ relation.getLeftTerm()
                    .getType().toString());
        }
    }

    private SelectValidatedQuery normalizeInnerSelect(
            SelectParsedQuery selectParsedQuery,
            Set<TableName> parentsTableNames) throws ValidationException {
        Validator validator = new Validator();
        try {
            return (SelectValidatedQuery) validator.validate(selectParsedQuery, parentsTableNames);
        } catch (IgnoreQueryException ex) {
            LOG.error(ex.getMessage());
            throw new BadFormatException(ex.getMessage());
        }
    }

    private void checkRelationFormatRight(Relation relation) throws ValidationException {
        switch (relation.getRightTerm().getType()) {
        case COLUMN:
        case STRING:
        case FLOATING_POINT:
        case BOOLEAN:
        case INTEGER:
            checkRightSelector(relation.getLeftTerm(), relation.getOperator(), relation.getRightTerm());
            break;
        case RELATION:
            RelationSelector relationSelector = (RelationSelector) relation.getRightTerm();
            checkRelationFormatLeft(relationSelector.getRelation());
            checkRelationFormatRight(relationSelector.getRelation());
            break;
        case ASTERISK:
            throw new BadFormatException("Not supported yet.");
        case FUNCTION:
            break;
        case SELECT:
            ExtendedSelectSelector extendedSelectSelector = (ExtendedSelectSelector) relation.getRightTerm();
            SelectValidatedQuery selectValidatedQuery = normalizeInnerSelect(
                    extendedSelectSelector.getSelectParsedQuery(),
                    fields.getTableNames());
            extendedSelectSelector.setSelectValidatedQuery(selectValidatedQuery);
            break;

        case GROUP:
            checkGroupSelector(relation.getLeftTerm(), relation.getOperator(), (GroupSelector) relation.getRightTerm());
            break;
        case LIST:
            break;
        default:
            throw new BadFormatException("Relations can't support this selector type: "+ relation.getRightTerm()
                    .getType().toString());
        }

    }

    /**
     * Validate the table of a parsed query.
     *
     * @param tableName The table name to validate
     * @throws ValidationException
     */
    public void checkTable(TableName tableName) throws ValidationException {
        if (!tableName.isCompletedName()) {
            tableName.setCatalogName(parsedQuery.getDefaultCatalog());
        }
        if (!MetadataManager.MANAGER.exists(tableName)) {
            throw new NotExistNameException(tableName);
        }
    }

    /**
     * Validate the column selectors.
     *
     * @param selector The selector
     * @throws ValidationException
     */
    public void checkColumnSelector(ColumnSelector selector) throws ValidationException {
        ColumnName columnName = applyAlias(selector);
        boolean columnFromVirtualTableFound = false;

        if (parsedQuery.getStatement().isSubqueryInc()) {
            //when the name is not completed and the table is not virtual
            if (!columnName.isCompletedName() || columnName.getTableName().isVirtual()) {
                columnFromVirtualTableFound = checkVirtualColumnSelector(selector, columnName);
            }
        }

        if (!columnFromVirtualTableFound) {
            if (columnName.isCompletedName()) {
                if (!MetadataManager.MANAGER.exists(columnName)) {
                    throw new NotValidColumnException(columnName);
                }
            } else {
                TableName searched = this.searchTableNameByColumn(columnName);
                columnName.setTableName(searched);
            }
        }

        fields.addColumnName(columnName, selector.getAlias());
        selector.setName(columnName);
        selector.setTableName(columnName.getTableName());
    }

    private boolean checkVirtualColumnSelector(ColumnSelector selector, ColumnName columnName)
            throws NotValidTableException {

        boolean columnFromVirtualTableFound = false;
        TableName tableName;

        if (columnName.getTableName() != null) {
            tableName = fields.getTableName(columnName.getTableName().getName());
            if (tableName == null) {
                throw new NotValidTableException(columnName.getTableName());
            }
        } else {
            tableName = new TableName(Constants.VIRTUAL_NAME, parsedQuery.getStatement().getSubqueryAlias());
        }

        for (Selector subquerySelector : subqueryNormalizator.getFields().getSelectors()) {

            if (subquerySelector.getAlias() != null) {
                columnFromVirtualTableFound = selector.getName().getName().equals(subquerySelector.getAlias());
            } else if (subquerySelector instanceof ColumnSelector) {
                columnFromVirtualTableFound = selector.getColumnName().getName()
                        .equals(subquerySelector.getColumnName().getName());
            }

            if (columnFromVirtualTableFound) {
                columnName.setTableName(tableName);
                selector.setTableName(tableName);
                columnFromVirtualTableFound = true;
                break;
            }
        }

        return columnFromVirtualTableFound;
    }

    private ColumnName applyAlias(ColumnName columnName) {
        ColumnName result = columnName;
        if (columnName.getTableName() != null && fields.existTableAlias(columnName.getTableName().getName())) {
            columnName.setTableName(fields.getTableName(columnName.getTableName().getName()));
        }

        if (fields.existColumnAlias(columnName.getName())) {
            result = fields.getColumnName(columnName.getName());
        }
        return result;
    }

    //TODO refactor -> merge with applyAlias(columnName)
    private ColumnName applyAlias(ColumnSelector columnSelector) {
        ColumnName result = columnSelector.getName();
        if (columnSelector.getName().getTableName() != null && fields
                .existTableAlias(columnSelector.getName().getTableName().getName())) {
            TableName table = fields.getTableName(columnSelector.getName().getTableName().getName());
            columnSelector.getName().setTableName(table);
            columnSelector.setTableName(table);
        }

        if (fields.existColumnAlias(columnSelector.getName().getName())) {
            result = fields.getColumnName(columnSelector.getName().getName());
        }
        return result;
    }

    /**
     * Search a table using a column name.
     *
     * @param columnName The column name
     * @return com.stratio.crossdata.common.data.ColumnName
     * @throws ValidationException
     */
    public TableName searchTableNameByColumn(ColumnName columnName) throws ValidationException {
        TableName selectTableName = null;
        if (columnName.isCompletedName()) {
            if (MetadataManager.MANAGER.exists(columnName)) {
                selectTableName = columnName.getTableName();
            }
        } else {
            if (columnName.getTableName() == null) {
                // Try to match with preferred tables
                selectTableName = matchColumn(columnName, fields.getPreferredTableNames());
                if (selectTableName == null) {
                    // Try to match with other tables from super queries
                    selectTableName = matchColumn(columnName, fields.getTableNames());
                }
            } else {
                for (TableName tableName : fields.getTableNames()) {
                    if (tableName.getName().equals(columnName.getTableName().getName())) {
                        columnName.setTableName(tableName);
                        selectTableName = tableName;
                    }
                }
                if (!columnName.isCompletedName()) {
                    throw new NotValidColumnException(columnName);
                }
            }
        }
        if (selectTableName == null) {
            throw new NotExistNameException(columnName);
        }
        return selectTableName;
    }

    private TableName matchColumn(ColumnName columnName, Set<TableName> tableNames) throws AmbiguousNameException {
        boolean tableFind = false;
        TableName selectTableName = null;
        for (TableName tableName : tableNames) {
            columnName.setTableName(tableName);
            if (MetadataManager.MANAGER.exists(columnName)) {
                if (tableFind) {
                    throw new AmbiguousNameException(columnName);
                }
                selectTableName = tableName;
                tableFind = true;
            } else {
                columnName.setTableName(null);
            }
        }
        if (selectTableName == null) {
            for (TableName tableName : tableNames) {
                if (tableName.isVirtual()) {
                    selectTableName = tableName;
                    break;
                }
            }
        }
        return selectTableName;
    }

    /**
     * Validate the conditions that have an asterisk.
     *
     * @return List of ColumnSelector
     */
    public List<Selector> checkAsteriskSelector() throws ValidationException {
        List<Selector> aSelectors = new ArrayList<>();
        SelectStatement selectStatement = parsedQuery.getStatement();
        Set<TableName> tableNames = fields.getPreferredTableNames();
        if ((tableNames == null) || (tableNames.isEmpty())) {
            tableNames = fields.getTableNames();
        }
        for (TableName table : tableNames) {
            if (table.isVirtual()) {
                for (Selector selector : selectStatement.getSubquery().getSelectExpression().getSelectorList()) {
                    ColumnName colName = new ColumnName(table, getVirtualAliasFromSelector(selector));
                    Selector defaultSelector = new ColumnSelector(colName);
                    defaultSelector.setAlias(selector.getAlias());
                    defaultSelector.setTableName(selectStatement.getTableName());
                    aSelectors.add(defaultSelector);
                    fields.addColumnName(colName, defaultSelector.getAlias());
                }
            } else {
                TableMetadata tableMetadata = MetadataManager.MANAGER.getTable(table);
                for (ColumnName columnName : tableMetadata.getColumns().keySet()) {
                    ColumnSelector selector = new ColumnSelector(columnName);
                    columnName.setTableName(table);
                    selector.setTableName(table);
                    aSelectors.add(selector);
                    fields.getColumnNames().add(columnName);
                }
            }
        }
        return aSelectors;
    }

    private String getVirtualAliasFromSelector(Selector selector) {
        String strColName;
        if (selector.getAlias() != null) {
            strColName = selector.getAlias();
        } else if (selector instanceof ColumnSelector) {
            strColName = selector.getColumnName().getName();
        } else {
            strColName = selector.getStringValue();
        }
        return strColName;
    }

    /**
     * Obtain a list of selectors that were validated.
     *
     * @param selectors The list of selectors to validate.
     * @return List of Selectors
     * @throws ValidationException
     */
    public List<Selector> checkListSelector(
            List<Selector> selectors) throws ValidationException {
        List<Selector> result = new ArrayList<>();
        TableName firstTableName = fields.getPreferredTableNames().iterator().next();
        for (Selector selector : selectors) {
            switch (selector.getType()) {
            case FUNCTION:
                FunctionSelector functionSelector = getFunctionSelector(firstTableName, (FunctionSelector) selector);
                result.add(functionSelector);
                break;
            case COLUMN:
                ColumnSelector columnSelector = getColumnSelector((ColumnSelector) selector);
                if(columnSelector.getAlias()!=null){
                    if(aliasSet.contains(columnSelector.getAlias())){
                        throw new BadFormatException("Duplicate Alias in select expression: " + columnSelector
                                .getAlias());
                    }else{
                        aliasSet.add(columnSelector.getAlias());
                    }
                }
                result.add(columnSelector);
                break;
            case RELATION:
                RelationSelector rs = getRelationSelector((RelationSelector) selector);
                result.add(rs);
                break;
            case ASTERISK:
                result.addAll(checkAsteriskSelector());
                break;
            case CASE_WHEN:
                CaseWhenSelector caseWhenSelector = getCaseWhenSelector((CaseWhenSelector) selector);
                result.add(caseWhenSelector);
                break;
            default:
                Selector defaultSelector = selector;
                defaultSelector.setTableName(firstTableName);
                result.add(defaultSelector);
                break;
            }
        }
        return result;
    }

    private FunctionSelector getFunctionSelector(TableName firstTableName, FunctionSelector selector)
            throws ValidationException {
        FunctionSelector functionSelector = (FunctionSelector) selector;
        getNormalizedFunctionAlias(functionSelector);
        checkFunctionSelector(functionSelector);
        functionSelector.setTableName(firstTableName);
        return functionSelector;
    }

    private CaseWhenSelector getCaseWhenSelector(CaseWhenSelector selector) throws ValidationException {
        CaseWhenSelector caseWhenSelector = (CaseWhenSelector) selector;
        List<Pair<List<AbstractRelation>, Selector>> restrictions = caseWhenSelector.getRestrictions();
        Selector lastSelector = restrictions.get(0).getRight();
        SelectorType lastType = lastSelector.getType();
        if (lastType == SelectorType.RELATION) {
            RelationSelector rSel = (RelationSelector) lastSelector;
            lastType = rSel.getReturningType();
        }
        for (Pair<List<AbstractRelation>, Selector> pair : restrictions) {
            List<AbstractRelation> relations = pair.getLeft();
            for (AbstractRelation relation : relations) {
                checkRelation(relation);
            }
            //TODO Check column compatibility
            if (lastSelector.getType() != pair.getRight().getType()) {
                throw new BadFormatException(
                        "All 'THEN' clauses in a CASE-WHEN select query must be of the same type");
            }
            lastSelector = pair.getRight();
        }
        SelectorType defaultType = caseWhenSelector.getDefaultValue().getType();
        if (defaultType != lastType) {
            if (COLUMN.equals(lastType) && !COLUMN.equals(defaultType)) {

                // Check columns
                List<Selector> normalizeSelector = checkListSelector(Arrays.asList(lastSelector));
                lastSelector = normalizeSelector.get(0);

                //TODO getRealDatatype from subquery
                if (!lastSelector.getTableName().isVirtual()) {
                    ColumnMetadata columnMetadata = MetadataManager.MANAGER
                            .getColumn(((ColumnSelector) lastSelector).getName());
                    //TODO delete the EQ operator workaround
                    checkCompatibility(columnMetadata, Operator.EQ, defaultType);
                }
            } else if (COLUMN.equals(defaultType) && !COLUMN.equals(lastType)) {

                // Check columns
                List<Selector> normalizeSelector = checkListSelector(
                        Arrays.asList(caseWhenSelector.getDefaultValue()));
                caseWhenSelector.setDefaultValue(normalizeSelector.get(0));

                //TODO getRealDatatype from subquery
                if (!lastSelector.getTableName().isVirtual()) {
                    ColumnMetadata columnMetadata = MetadataManager.MANAGER
                            .getColumn(((ColumnSelector) caseWhenSelector.getDefaultValue()).getName());
                    //TODO delete the EQ operator workaround
                    checkCompatibility(columnMetadata, Operator.EQ, lastType);
                }
            } else if (lastType.isNumeric() && defaultType.isNumeric()) {
                if (lastSelector.getType() == SelectorType.RELATION) {
                    checkListSelector(Arrays.asList(lastSelector));
                }
                if (defaultType == SelectorType.RELATION) {
                    checkListSelector(Arrays.asList(caseWhenSelector.getDefaultValue()));
                }
            } else {
                throw new BadFormatException(
                        "ELSE clause in a CASE-WHEN select query must be of the same type of when clauses");
            }
        }
        return caseWhenSelector;
    }

    private RelationSelector getRelationSelector(RelationSelector selector) throws ValidationException {
        RelationSelector rs = (RelationSelector) selector;
        List<Selector> leftTerm = checkListSelector(
                Collections.singletonList(rs.getRelation().getLeftTerm()));
        List<Selector> rightTerm = checkListSelector(
                Collections.singletonList(rs.getRelation().getRightTerm()));
        rs.getRelation().setLeftTerm(leftTerm.get(0));
        if ((rightTerm != null) && (!rightTerm.isEmpty())) {
            rs.getRelation().setRightTerm(rightTerm.get(0));
        }
        return rs;
    }

    private ColumnSelector getColumnSelector(ColumnSelector selector) throws ValidationException {
        ColumnSelector columnSelector = (ColumnSelector) selector;
        checkColumnSelector(columnSelector);

        Iterator<TableName> tableNameIterator = fields.getPreferredTableNames().iterator();

        TableName currentTableName = null;
        boolean tableFound = false;
        while (tableNameIterator.hasNext() && !tableFound) {
            currentTableName = tableNameIterator.next();
            //TODO Tablename should be always distinct from null
            if (columnSelector.getTableName() != null) {
                if (!columnSelector.getName().getTableName().getName().equals(currentTableName.getName())) {
                    if (!tableNameIterator.hasNext()) {
                        throw new NotValidTableException(columnSelector.getName().getTableName());
                    }
                } else {

                    tableFound = !(columnSelector.getName().getTableName().getCatalogName() != null
                            && !columnSelector.getName().getTableName().getCatalogName().getName()
                            .equals(currentTableName.getCatalogName().getName()));
                    if (!tableFound && !tableNameIterator.hasNext()) {
                        throw new NotValidCatalogException(columnSelector.getTableName().getCatalogName());
                    }
                }
            }
        }
        columnSelector.setTableName(currentTableName);
        return columnSelector;
    }

    private FunctionSelector getNormalizedFunctionAlias(FunctionSelector functionSelector) {
        if (aliasFunctionMap.containsKey(functionSelector.getAlias())) {
            int index = aliasFunctionMap.get(functionSelector.getAlias());
            aliasFunctionMap.put(functionSelector.getAlias(), index + 1);
            functionSelector.setAlias(functionSelector.getAlias() + Integer.toString(index + 1));
        } else {
            aliasFunctionMap.put(functionSelector.getAlias(), 0);
        }
        return functionSelector;
    }



    /**
     * Validate the Functions Selectors of a parsed query.
     *
     * @param functionSelector The includes Selector to validate.
     * @throws ValidationException
     */
    private void checkFunctionSelector(FunctionSelector functionSelector) throws ValidationException {
        // Check columns
        List<Selector> normalizeSelector = checkListSelector(functionSelector.getFunctionColumns());
        functionSelector.getFunctionColumns().clear();
        functionSelector.getFunctionColumns().addAll(normalizeSelector);
    }

    private void checkRightSelector(
            Selector leftTerm,
            Operator operator,
            Selector rightTerm) throws ValidationException {
        if (leftTerm instanceof ColumnSelector) {
            ColumnName name = ((ColumnSelector) leftTerm).getName();
            if (!parsedQuery.getStatement().isSubqueryInc()) {
                // Get column type from MetadataManager
                ColumnMetadata columnMetadata = MetadataManager.MANAGER.getColumn(name);
                SelectorType rightTermType = rightTerm.getType();
                if (rightTerm.getType() == SelectorType.COLUMN) {
                    ColumnSelector columnSelector = addTableNameToRightSelector(rightTerm);
                    ColumnMetadata columnMetadataRightTerm = MetadataManager.MANAGER
                            .getColumn(columnSelector.getName());
                    if (columnMetadataRightTerm.getColumnType().getDataType() != DataType.NATIVE) {
                        rightTermType = convertMetadataTypeToSelectorType(columnMetadataRightTerm.getColumnType());
                    }
                }
                // Create compatibilities table for ColumnType, Operator and SelectorType
                if (operator != Operator.MATCH) {
                    checkCompatibility(columnMetadata, operator, rightTermType);
                }
            }
        } else {
            if (rightTerm.getType() == SelectorType.COLUMN) {
                addTableNameToRightSelector(rightTerm);
            }
        }
    }

    private void checkGroupSelector(Selector leftTerm, Operator operator,
            GroupSelector rightTerm) throws ValidationException {
        if (leftTerm instanceof ColumnSelector) {
            if (operator == Operator.BETWEEN || operator == Operator.NOT_BETWEEN) {
                ColumnName name = ((ColumnSelector) leftTerm).getName();
                ColumnMetadata columnMetadata = MetadataManager.MANAGER.getColumn(name);
                String leftType = "";

                switch (columnMetadata.getColumnType().getCrossdataType().toUpperCase()) {
                case "BIGINT":
                case "FLOAT":
                case "INT":
                case "TEXT":
                case "VARCHAR":
                case "NATIVE":
                case "LIST":
                    break;
                default:
                    throw new NotMatchDataTypeException(leftTerm.getColumnName());
                }
                String firstType = rightTerm.getFirstValue().getType().toString();
                String lastType = rightTerm.getLastValue().getType().toString();

                //TODO drop Operator.EQ workaround. Use checkCompatibility above
                if (!lastType.equals(FUNCTION.toString()) && !lastType.equals(RELATION.toString())) {
                    checkCompatibility(columnMetadata, Operator.EQ, rightTerm.getLastValue().getType());
                }

                if (!firstType.equals(FUNCTION.toString()) && !firstType.equals(RELATION.toString())) {
                    checkCompatibility(columnMetadata, Operator.EQ, rightTerm.getFirstValue().getType());
                }
            } else {
                throw new BadFormatException("Left Term must be a column in a Group Filter.");
            }
        }

    }

    private ColumnSelector addTableNameToRightSelector(Selector rightTerm) throws ValidationException {
        ColumnSelector columnSelector = (ColumnSelector) rightTerm;
        ColumnName columnName = applyAlias(columnSelector.getName());
        columnSelector.setName(columnName);

        TableName foundTableName = this.searchTableNameByColumn(columnSelector.getName());
        columnSelector.getName().setTableName(foundTableName);
        return columnSelector;
    }

    private SelectorType convertMetadataTypeToSelectorType(ColumnType columnType) throws ValidationException {
        SelectorType selectorType = null;
        switch (columnType.getDataType()) {
        case INT:
        case BIGINT:
            selectorType = SelectorType.INTEGER;
            break;
        case DOUBLE:
        case FLOAT:
            selectorType = SelectorType.FLOATING_POINT;
            break;
        case TEXT:
        case VARCHAR:
            selectorType = SelectorType.STRING;
            break;
        case BOOLEAN:
            selectorType = SelectorType.BOOLEAN;
            break;
        case NATIVE:
        case SET:
        case LIST:
        case MAP:
            throw new BadFormatException("Type " + columnType + " not supported yet.");
        }
        return selectorType;
    }

    private void checkCompatibility(ColumnMetadata column, Operator operator, SelectorType valueType)
            throws ValidationException {
        switch (column.getColumnType().getDataType()) {
        case BOOLEAN:
            checkBooleanCompatibility(column, operator, valueType);
            break;
        case INT:
        case BIGINT:
        case DOUBLE:
        case FLOAT:
            checkNumericCompatibility(column, valueType);
            break;
        case TEXT:
        case VARCHAR:
            checkStringCompatibility(column, operator, valueType);
            break;
        case SET:
        case LIST:
        case MAP:
            throw new BadFormatException("Collections not supported yet.");

        case NATIVE:
            //we don't check native types
            break;
        }
    }

    private void checkBooleanCompatibility(ColumnMetadata column, Operator operator, SelectorType valueType)
            throws ValidationException {
        if ((operator != Operator.EQ) && (operator != Operator.DISTINCT)) {
            throw new BadFormatException("Boolean relations only accept equal and distinct operator.");
        }
        if (valueType != SelectorType.BOOLEAN) {
            throw new NotMatchDataTypeException(column.getName());
        }
    }

    private void checkNumericCompatibility(ColumnMetadata column, SelectorType valueType) throws ValidationException {
        if ((valueType != SelectorType.INTEGER) && (valueType != SelectorType.FLOATING_POINT)) {
            throw new NotMatchDataTypeException(column.getName());
        }
    }

    private void checkStringCompatibility(ColumnMetadata column, Operator operator, SelectorType valueType)
            throws ValidationException {
        if (valueType != SelectorType.STRING) {
            throw new NotMatchDataTypeException(column.getName());
        }

        if (operator == Operator.MATCH) {
            if (valueType != SelectorType.STRING) {
                throw new BadFormatException("MATCH operator only accepts comparison with string literals.");
            }

            TableMetadata tableMetadata = MetadataManager.MANAGER.getTable(column.getName().getTableName());
            Map<IndexName, IndexMetadata> indexes = tableMetadata.getIndexes();
            if (indexes == null || indexes.isEmpty()) {
                throw new BadFormatException(
                        "Table " + column.getName().getTableName() + " doesn't contain any index.");
            }

            boolean indexFound = false;
            Collection<IndexMetadata> indexesMetadata = indexes.values();
            Iterator<IndexMetadata> iter = indexesMetadata.iterator();
            while (iter.hasNext() && !indexFound) {
                IndexMetadata indexMetadata = iter.next();
                if (indexMetadata.getColumns().containsKey(column.getName())) {
                    if (indexMetadata.getType() != IndexType.FULL_TEXT) {
                        throw new BadFormatException("MATCH operator can be only applied to FULL_TEXT indexes.");
                    } else {
                        indexFound = true;
                    }
                }
            }
            if (!indexFound) {
                throw new BadFormatException("No index was found for the MATCH operator.");
            }
        } else if ( !operator.isInGroup (Operator.Group.COMPARATOR)){
            throw new BadFormatException("String relations does not accept " + operator.toString() + " operator.");
        }

    }
}
