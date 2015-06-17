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

package com.stratio.crossdata.common.metadata;

/**
 * Operations supported by an {@link com.stratio.crossdata.common.connector.IConnector}.
 */
public enum Operations {

    /**
     * The engine supports creating new catalogs.
     */
    CREATE_CATALOG("CREATE_CATALOG"),

    /**
     * The engine supports catalog alteration given an existing catalog.
     */
    ALTER_CATALOG("ALTER_CATALOG"),

    /**
     * The engine supports creating deleting existing catalogs.
     */
    DROP_CATALOG("DROP_CATALOG"),

    /**
     * The engine supports creating new tables given an existing catalog.
     */
    CREATE_TABLE("CREATE_TABLE"),

    /**
     * The engine supports table alteration given an existing table.
     */
    ALTER_TABLE("ALTER_TABLE"),

    /**
     * The engine supports deleting new tables given an existing catalog.
     */
    DROP_TABLE("DROP_TABLE"),

    /**
     * The engine supports Metadata Discovery.
     */
    IMPORT_METADATA("IMPORT_METADATA"),

    /**
     * The engine supports unconditional updates.
     */
    UPDATE_NO_FILTERS("UPDATE_NO_FILTERS"),

    /**
     * The engine supports update operations on columns
     * that are part of the primary key with an equal operator.
     */
    UPDATE_PK_EQ("UPDATE_PK_EQ"),

    /**
     * The engine supports update operations on columns
     * that are part of the primary key with greater than operator.
     */
    UPDATE_PK_GT("UPDATE_PK_GT"),

    /**
     * The engine supports update operations on columns
     * that are part of the primary key with less than operator.
     */
    UPDATE_PK_LT("UPDATE_PK_LT"),

    /**
     * The engine supports update operations on columns
     * that are part of the primary key with greater than or equal operator.
     */
    UPDATE_PK_GET("UPDATE_PK_GET"),

    /**
     * The engine supports update operations on columns
     * that are part of the primary key with less than or equal operator.
     */
    UPDATE_PK_LET("UPDATE_PK_LET"),

    /**
     * The engine supports update operations on columns
     * that are part of the primary key with distinct operator.
     */
    UPDATE_PK_DISTINCT("UPDATE_PK_DISTINCT"),

    /**
     * The engine supports update operations on columns
     * that are part of the primary key with like operator.
     */
    UPDATE_PK_LIKE("UPDATE_PK_LIKE"),

    /**
     * The engine supports update operations on columns
     * that are part of the primary key with not like operator.
     */
    UPDATE_PK_NOT_LIKE("UPDATE_PK_NOT_LIKE"),

    /**
     * The engine supports update operations on columns
     * that are part of the primary key with in operator.
     */
    UPDATE_PK_IN("UPDATE_PK_IN"),

    /**
     * The engine supports update operations on columns
     * that are part of the primary key with not in operator.
     */
    UPDATE_PK_NOT_IN("UPDATE_PK_NOT_IN"),

    /**
     * The engine supports update operations on columns
     * that are part of the primary key with between operator.
     */
    UPDATE_PK_BETWEEN("UPDATE_PK_BETWEEN"),

    /**
     * The engine supports update operations on columns
     * that are part of the primary key with not between operator.
     */
    UPDATE_PK_NOT_BETWEEN("UPDATE_PK_NOT_BETWEEN"),

    /**
     * The engine supports update operations on columns
     * that are not indexed by the underlying datastore with an equal operator.
     */
    UPDATE_NON_INDEXED_EQ("UPDATE_NON_INDEXED_EQ"),

    /**
     * The engine supports update operations on columns
     * that are not indexed by the underlying datastore with a greater than operator.
     */
    UPDATE_NON_INDEXED_GT("UPDATE_NON_INDEXED_GT"),

    /**
     * The engine supports update operations on columns
     * that are not indexed by the underlying datastore with a less than operator.
     */
    UPDATE_NON_INDEXED_LT("UPDATE_NON_INDEXED_LT"),

    /**
     * The engine supports update operations on columns
     * that are not indexed by the underlying datastore with greater than or equal operator.
     */
    UPDATE_NON_INDEXED_GET("UPDATE_NON_INDEXED_GET"),

    /**
     * The engine supports update operations on columns
     * that are not indexed by the underlying datastore with less than or equal operator.
     */
    UPDATE_NON_INDEXED_LET("UPDATE_NON_INDEXED_LET"),

    /**
     * The engine supports update operations on columns
     * that are not indexed by the underlying datastore with distinct operator.
     */
    UPDATE_NON_INDEXED_DISTINCT("UPDATE_NON_INDEXED_DISTINCT"),

    /**
     * The engine supports update operations on columns
     * that are not indexed by the underlying datastore with like operator.
     */
    UPDATE_NON_INDEXED_LIKE("UPDATE_NON_INDEXED_LIKE"),

    /**
     * The engine supports update operations on columns
     * that are not indexed by the underlying datastore with not like operator.
     */
    UPDATE_NON_INDEXED_NOT_LIKE("UPDATE_NON_INDEXED_NOT_LIKE"),

    /**
     * The engine supports update operations on columns
     * that are not indexed by the underlying datastore with in operator.
     */
    UPDATE_NON_INDEXED_IN("UPDATE_NON_INDEXED_IN"),

    /**
     * The engine supports update operations on columns
     * that are not indexed by the underlying datastore with not in operator.
     */
    UPDATE_NON_INDEXED_NOT_IN("UPDATE_NON_INDEXED_NOT_IN"),

    /**
     * The engine supports update operations on columns
     * that are not indexed by the underlying datastore with between operator.
     */
    UPDATE_NON_INDEXED_BETWEEN("UPDATE_NON_INDEXED_BETWEEN"),

    /**
     * The engine supports update operations on columns
     * that are not indexed by the underlying datastore with less than or equal operator.
     */
    UPDATE_NON_INDEXED_NOT_BETWEEN("UPDATE_NON_INDEXED_NOT_BETWEEN"),

    /**
     * The engine supports update operations on columns
     * that have an associated index in the underlying datastore with an equal operator.
     */
    UPDATE_INDEXED_EQ("UPDATE_INDEXED_EQ"),

    /**
     * The engine supports update operations on columns
     * that have an associated index in the underlying datastore with a greater than operator.
     */
    UPDATE_INDEXED_GT("UPDATE_INDEXED_GT"),

    /**
     * The engine supports update operations on columns
     * that have an associated index in the underlying datastore with a less than operator.
     */
    UPDATE_INDEXED_LT("UPDATE_INDEXED_LT"),

    /**
     * The engine supports update operations on columns
     * that have an associated index in the underlying datastore with a greater than or equal operator.
     */
    UPDATE_INDEXED_GET("UPDATE_INDEXED_GET"),

    /**
     * The engine supports update operations on columns
     * that have an associated index in the underlying datastore with a less than or equal operator.
     */
    UPDATE_INDEXED_LET("UPDATE_INDEXED_LET"),

    /**
     * The engine supports update operations on columns
     * that have an associated index in the underlying datastore with distinct operator.
     */
    UPDATE_INDEXED_DISTINCT("UPDATE_INDEXED_DISTINCT"),

    /**
     * The engine supports update operations on columns
     * that have an associated index in the underlying datastore with like operator.
     */
    UPDATE_INDEXED_LIKE("UPDATE_INDEXED_LIKE"),

    /**
     * The engine supports update operations on columns
     * that have an associated index in the underlying datastore with not like operator.
     */
    UPDATE_INDEXED_NOT_LIKE("UPDATE_INDEXED_NOT_LIKE"),

    /**
     * The engine supports update operations on columns
     * that have an associated index in the underlying datastore with in operator.
     */
    UPDATE_INDEXED_IN("UPDATE_INDEXED_IN"),

    /**
     * The engine supports update operations on columns
     * that have an associated index in the underlying datastore with not in operator.
     */
    UPDATE_INDEXED_NOT_IN("UPDATE_INDEXED_NOT_IN"),

    /**
     * The engine supports update operations on columns
     * that have an associated index in the underlying datastore with between operator.
     */
    UPDATE_INDEXED_BETWEEN("UPDATE_INDEXED_BETWEEN"),

    /**
     * The engine supports update operations on columns
     * that have an associated index in the underlying datastore with not between operator.
     */
    UPDATE_INDEXED_NOT_BETWEEN("UPDATE_INDEXED_NOT_BETWEEN"),

    /**
     * The engine supports update operations using
     * a includes as part of a relation using with an equal operator.
     */
    UPDATE_FUNCTION_EQ("UPDATE_FUNCTION_EQ"),

    /**
     * The engine supports update operations using
     * a includes as part of a relation using with a greater than operator.
     */
    UPDATE_FUNCTION_GT("UPDATE_FUNCTION_GT"),

    /**
     * The engine supports update operations using
     * a includes as part of a relation using with a less than operator.
     */
    UPDATE_FUNCTION_LT("UPDATE_FUNCTION_LT"),

    /**
     * The engine supports update operations using
     * a includes as part of a relation using with a greater than or equal operator.
     */
    UPDATE_FUNCTION_GET("UPDATE_FUNCTION_GET"),

    /**
     * The engine supports update operations using
     * a includes as part of a relation using with a less than or equal operator.
     */
    UPDATE_FUNCTION_LET("UPDATE_FUNCTION_LET"),

    /**
     * The engine supports update operations using
     * a includes as part of a relation using with distinct operator.
     */
    UPDATE_FUNCTION_DISTINCT("UPDATE_FUNCTION_DISTINCT"),

    /**
     * The engine supports update operations using
     * a includes as part of a relation using with like operator.
     */
    UPDATE_FUNCTION_LIKE("UPDATE_FUNCTION_LIKE"),

    /**
     * The engine supports update operations using
     * a includes as part of a relation using with not like operator.
     */
    UPDATE_FUNCTION_NOT_LIKE("UPDATE_FUNCTION_NOT_LIKE"),

    /**
     * The engine supports update operations using
     * a includes as part of a relation using with in operator.
     */
    UPDATE_FUNCTION_IN("UPDATE_FUNCTION_IN"),

    /**
     * The engine supports update operations using
     * a includes as part of a relation using with not in operator.
     */
    UPDATE_FUNCTION_NOT_IN("UPDATE_FUNCTION_NOT_IN"),

    /**
     * The engine supports update operations using
     * a includes as part of a relation using with between operator.
     */
    UPDATE_FUNCTION_BETWEEN("UPDATE_FUNCTION_BETWEEN"),

    /**
     * The engine supports update operations using
     * a includes as part of a relation using with not between operator.
     */
    UPDATE_FUNCTION_NOT_BETWEEN("UPDATE_FUNCTION_NOT_BETWEEN"),

    /**
     * The engine supports deleting all the data from a table without removing its metadata.
     */
    TRUNCATE_TABLE("TRUNCATE_TABLE"),

    /**
     * The engine supports inserting data in existing tables.
     */
    INSERT("INSERT"),

    /**
     * For Data stores whose insert operation behave as upsert by default, this operation points out that the
     * connector is capable of behaving as a "classic" insert.
     */
    INSERT_IF_NOT_EXISTS("INSERT_IF_NOT_EXISTS"),

    /**
     * The engine supports inserting data via message brokers using custom serializers.
     */
    INSERT_THROUGH_MESSAGE_BROKER("INSERT_THROUGH_MESSAGE_BROKER"),

    /**
     * The engine supports unconditional deletes.
     */
    DELETE_NO_FILTERS("DELETE_NO_FILTERS"),

    /**
     * The engine supports delete operations on columns
     * that are part of the primary key with an equal operator.
     */
    DELETE_PK_EQ("DELETE_PK_EQ"),

    /**
     * The engine supports delete operations on columns
     * that are part of the primary key with greater than operator.
     */
    DELETE_PK_GT("DELETE_PK_GT"),

    /**
     * The engine supports delete operations on columns
     * that are part of the primary key with less than operator.
     */
    DELETE_PK_LT("DELETE_PK_LT"),

    /**
     * The engine supports delete operations on columns
     * that are part of the primary key with greater than or equal operator.
     */
    DELETE_PK_GET("DELETE_PK_GET"),

    /**
     * The engine supports delete operations on columns
     * that are part of the primary key with less than or equal operator.
     */
    DELETE_PK_LET("DELETE_PK_LET"),

    /**
     * The engine supports delete operations on columns
     * that are part of the primary key with distinct operator.
     */
    DELETE_PK_DISTINCT("DELETE_PK_DISTINCT"),

    /**
     * The engine supports delete operations on columns
     * that are part of the primary key with like operator.
     */
    DELETE_PK_LIKE("DELETE_PK_LIKE"),

    /**
     * The engine supports delete operations on columns
     * that are part of the primary key with not like operator.
     */
    DELETE_PK_NOT_LIKE("DELETE_PK_NOT_LIKE"),

    /**
     * The engine supports delete operations on columns
     * that are part of the primary key with in operator.
     */
    DELETE_PK_IN("DELETE_PK_IN"),

    /**
     * The engine supports delete operations on columns
     * that are part of the primary key with not in operator.
     */
    DELETE_PK_NOT_IN("DELETE_PK_NOT_IN"),

    /**
     * The engine supports delete operations on columns
     * that are part of the primary key with between operator.
     */
    DELETE_PK_BETWEEN("DELETE_PK_BETWEEN"),

    /**
     * The engine supports delete operations on columns
     * that are part of the primary key with not between operator.
     */
    DELETE_PK_NOT_BETWEEN("DELETE_PK_NOT_BETWEEN"),

    /**
     * The engine supports delete operations on columns
     * that are not indexed by the underlying datastore with an equal operator.
     */
    DELETE_NON_INDEXED_EQ("DELETE_NON_INDEXED_EQ"),

    /**
     * The engine supports delete operations on columns
     * that are not indexed by the underlying datastore with a greater than operator.
     */
    DELETE_NON_INDEXED_GT("DELETE_NON_INDEXED_GT"),

    /**
     * The engine supports delete operations on columns
     * that are not indexed by the underlying datastore with a less than operator.
     */
    DELETE_NON_INDEXED_LT("DELETE_NON_INDEXED_LT"),

    /**
     * The engine supports delete operations on columns
     * that are not indexed by the underlying datastore with greater than or equal operator.
     */
    DELETE_NON_INDEXED_GET("DELETE_NON_INDEXED_GET"),

    /**
     * The engine supports delete operations on columns
     * that are not indexed by the underlying datastore with less than or equal operator.
     */
    DELETE_NON_INDEXED_LET("DELETE_NON_INDEXED_LET"),

    /**
     * The engine supports delete operations on columns
     * that are not indexed by the underlying datastore with distinct operator.
     */
    DELETE_NON_INDEXED_DISTINCT("DELETE_NON_INDEXED_DISTINCT"),

    /**
     * The engine supports delete operations on columns
     * that are not indexed by the underlying datastore with like operator.
     */
    DELETE_NON_INDEXED_LIKE("DELETE_NON_INDEXED_LIKE"),

    /**
     * The engine supports delete operations on columns
     * that are not indexed by the underlying datastore with not like operator.
     */
    DELETE_NON_INDEXED_NOT_LIKE("DELETE_NON_INDEXED_NOT_LIKE"),

    /**
     * The engine supports delete operations on columns
     * that are not indexed by the underlying datastore with in operator.
     */
    DELETE_NON_INDEXED_IN("DELETE_NON_INDEXED_IN"),

    /**
     * The engine supports delete operations on columns
     * that are not indexed by the underlying datastore with not in operator.
     */
    DELETE_NON_INDEXED_NOT_IN("DELETE_NON_INDEXED_NOT_IN"),

    /**
     * The engine supports delete operations on columns
     * that are not indexed by the underlying datastore with between operator.
     */
    DELETE_NON_INDEXED_BETWEEN("DELETE_NON_INDEXED_BETWEEN"),

    /**
     * The engine supports delete operations on columns
     * that are not indexed by the underlying datastore with not between operator.
     */
    DELETE_NON_INDEXED_NOT_BETWEEN("DELETE_NON_INDEXED_NOT_BETWEEN"),

    /**
     * The engine supports delete operations on columns
     * that have an associated index in the underlying datastore with an equal operator.
     */
    DELETE_INDEXED_EQ("DELETE_INDEXED_EQ"),

    /**
     * The engine supports delete operations on columns
     * that have an associated index in the underlying datastore with a greater than operator.
     */
    DELETE_INDEXED_GT("DELETE_INDEXED_GT"),

    /**
     * The engine supports delete operations on columns
     * that have an associated index in the underlying datastore with a less than operator.
     */
    DELETE_INDEXED_LT("DELETE_INDEXED_LT"),

    /**
     * The engine supports delete operations on columns
     * that have an associated index in the underlying datastore with a greater than or equal operator.
     */
    DELETE_INDEXED_GET("DELETE_INDEXED_GET"),

    /**
     * The engine supports delete operations on columns
     * that have an associated index in the underlying datastore with a less than or equal operator.
     */
    DELETE_INDEXED_LET("DELETE_INDEXED_LET"),

    /**
     * The engine supports delete operations on columns
     * that have an associated index in the underlying datastore with distinct operator.
     */
    DELETE_INDEXED_DISTINCT("DELETE_INDEXED_DISTINCT"),

    /**
     * The engine supports delete operations on columns
     * that have an associated index in the underlying datastore with like operator.
     */
    DELETE_INDEXED_LIKE("DELETE_INDEXED_LIKE"),

    /**
     * The engine supports delete operations on columns
     * that have an associated index in the underlying datastore with not like operator.
     */
    DELETE_INDEXED_NOT_LIKE("DELETE_INDEXED_NOT_LIKE"),

    /**
     * The engine supports delete operations on columns
     * that have an associated index in the underlying datastore with inl operator.
     */
    DELETE_INDEXED_IN("DELETE_INDEXED_IN"),

    /**
     * The engine supports delete operations on columns
     * that have an associated index in the underlying datastore with not in operator.
     */
    DELETE_INDEXED_NOT_IN("DELETE_INDEXED_NOT_IN"),

    /**
     * The engine supports delete operations on columns
     * that have an associated index in the underlying datastore with between operator.
     */
    DELETE_INDEXED_BETWEEN("DELETE_INDEXED_BETWEEN"),

    /**
     * The engine supports delete operations on columns
     * that have an associated index in the underlying datastore with not between operator.
     */
    DELETE_INDEXED_NOT_BETWEEN("DELETE_INDEXED_NOT_BETWEEN"),
    /**
     * The engine supports delete operations using
     * a includes as part of a relation using with an equal operator.
     */
    DELETE_FUNCTION_EQ("DELETE_FUNCTION_EQ"),

    /**
     * The engine supports delete operations using
     * a includes as part of a relation using with a greater than operator.
     */
    DELETE_FUNCTION_GT("DELETE_FUNCTION_GT"),

    /**
     * The engine supports delete operations using
     * a includes as part of a relation using with a less than operator.
     */
    DELETE_FUNCTION_LT("DELETE_FUNCTION_LT"),

    /**
     * The engine supports delete operations using
     * a includes as part of a relation using with a greater than or equal operator.
     */
    DELETE_FUNCTION_GET("DELETE_FUNCTION_GET"),

    /**
     * The engine supports delete operations using
     * a includes as part of a relation using with a less than or equal operator.
     */
    DELETE_FUNCTION_LET("DELETE_FUNCTION_LET"),

    /**
     * The engine supports delete operations using
     * a includes as part of a relation using with distinct operator.
     */
    DELETE_FUNCTION_DISTINCT("DELETE_FUNCTION_DISTINCT"),

    /**
     * The engine supports delete operations using
     * a includes as part of a relation using with like operator.
     */
    DELETE_FUNCTION_LIKE("DELETE_FUNCTION_LIKE"),

    /**
     * The engine supports delete operations using
     * a includes as part of a relation using with not like operator.
     */
    DELETE_FUNCTION_NOT_LIKE("DELETE_FUNCTION_NOT_LIKE"),

    /**
     * The engine supports delete operations using
     * a includes as part of a relation using with in operator.
     */
    DELETE_FUNCTION_IN("DELETE_FUNCTION_IN"),

    /**
     * The engine supports delete operations using
     * a includes as part of a relation using with not in operator.
     */
    DELETE_FUNCTION_NOT_IN("DELETE_FUNCTION_NOT_IN"),

    /**
     * The engine supports delete operations using
     * a includes as part of a relation using with between operator.
     */
    DELETE_FUNCTION_BETWEEN("DELETE_FUNCTION_BETWEEN"),

    /**
     * The engine supports delete operations using
     * a includes as part of a relation using with not between operator.
     */
    DELETE_FUNCTION_NOT_BETWEEN("DELETE_FUNCTION_NOT_BETWEEN"),

    /**
     * The engine supports index creation from existing tables.
     */
    CREATE_INDEX("CREATE_INDEX"),

    /**
     * The engine supports index deletion from existing tables.
     */
    DROP_INDEX("DROP_INDEX"),

    /**
     * The engine supports retrieving a set of columns from a specific table.
     */
    PROJECT("PROJECT"),

    /**
     * The engine supports asynchronous query execution.
     */
    ASYNC_QUERY("ASYNC_QUERY"),

    /**
     * The engine supports aliasing output names.
     */
    SELECT_OPERATOR("SELECT_OPERATOR"),

    /**
     * The engine supports {@link com.stratio.crossdata.common.statements.structures.window.Window} logical
     * plans for streaming-like datastores.
     */
    SELECT_WINDOW("SELECT_WINDOW"),

    /**
     * The engine supports limiting the number of results returned in a query.
     */
    SELECT_LIMIT("SELECT_LIMIT"),

    /**
     * The engine supports case when selector.
     */
    SELECT_CASE_WHEN("SELECT_CASE_WHEN"),

    /**
     * The engine supports inner joins.
     */
    SELECT_INNER_JOIN("SELECT_INNER_JOIN"),

    /**
     * The engine supports inner joins with partial results.
     */
    SELECT_INNER_JOIN_PARTIALS_RESULTS("SELECT_INNER_JOIN_PARTIALS_RESULTS"),

    /**
     * The engine supports order by clauses.
     */
    SELECT_ORDER_BY("SELECT_ORDER_BY"),

    /**
     * The engine supports group by clauses.
     */
    SELECT_GROUP_BY("SELECT_GROUP_BY"),

    /**
     * The engine supports aggregator operations (e.g., sum, avg, etc.) on a Select statement.
     */
    SELECT_FUNCTIONS("SELECT_FUNCTIONS"),

    /**
     * The engine supports {@link com.stratio.crossdata.common.logicalplan.Filter} operations on columns
     * that are part of the primary key with an equal operator.
     */
    FILTER_PK_EQ("FILTER_PK_EQ"),

    /**
     * The engine supports {@link com.stratio.crossdata.common.logicalplan.Filter} operations on columns
     * that are part of the primary key with greater than operator.
     */
    FILTER_PK_GT("FILTER_PK_GT"),

    /**
     * The engine supports {@link com.stratio.crossdata.common.logicalplan.Filter} operations on columns
     * that are part of the primary key with less than operator.
     */
    FILTER_PK_LT("FILTER_PK_LT"),

    /**
     * The engine supports {@link com.stratio.crossdata.common.logicalplan.Filter} operations on columns
     * that are part of the primary key with greater than or equal operator.
     */
    FILTER_PK_GET("FILTER_PK_GET"),

    /**
     * The engine supports {@link com.stratio.crossdata.common.logicalplan.Filter} operations on columns
     * that are part of the primary key with less than or equal operator.
     */
    FILTER_PK_LET("FILTER_PK_LET"),

    /**
     * The engine supports {@link com.stratio.crossdata.common.logicalplan.Filter} operations on columns
     * that are part of the primary key with distinct operator.
     */
    FILTER_PK_DISTINCT("FILTER_PK_DISTINCT"),

    /**
     * The engine supports {@link com.stratio.crossdata.common.logicalplan.Filter} operations on columns
     * that are part of the primary key with match operator.
     */
    FILTER_PK_MATCH("FILTER_PK_MATCH"),

    /**
     * The filter of like operator over a PK.
     */
    FILTER_PK_LIKE("FILTER_PK_LIKE"),

    /**
     * The engine supports {@link com.stratio.crossdata.common.logicalplan.Filter} operations on columns
     * that are part of the primary key with not like operator.
     */
    FILTER_PK_NOT_LIKE("FILTER_PK_NOT_LIKE"),

    /**
     * The engine supports {@link com.stratio.crossdata.common.logicalplan.Filter} operations on columns
     * that are part of the primary key with in operator.
     */
    FILTER_PK_IN("FILTER_PK_IN"),

    /**
     * The engine supports {@link com.stratio.crossdata.common.logicalplan.Filter} operations on columns
     * that are part of the primary key with not in operator.
     */
    FILTER_PK_NOT_IN("FILTER_PK_NOT_IN"),

    /**
     * The engine supports {@link com.stratio.crossdata.common.logicalplan.Filter} operations on columns
     * that are part of the primary key with between operator.
     */
    FILTER_PK_BETWEEN("FILTER_PK_BETWEEN"),

    /**
     * The engine supports {@link com.stratio.crossdata.common.logicalplan.Filter} operations on columns
     * that are part of the primary key with not between operator.
     */
    FILTER_PK_NOT_BETWEEN("FILTER_PK_NOT_BETWEEN"),

    /**
     * The engine supports {@link com.stratio.crossdata.common.logicalplan.Filter} operations on columns
     * that are not indexed by the underlying datastore with an equal operator.
     */
    FILTER_NON_INDEXED_EQ("FILTER_NON_INDEXED_EQ"),

    /**
     * The engine supports {@link com.stratio.crossdata.common.logicalplan.Filter} operations on columns
     * that are not indexed by the underlying datastore with a greater than operator.
     */
    FILTER_NON_INDEXED_GT("FILTER_NON_INDEXED_GT"),

    /**
     * The engine supports {@link com.stratio.crossdata.common.logicalplan.Filter} operations on columns
     * that are not indexed by the underlying datastore with a less than operator.
     */
    FILTER_NON_INDEXED_LT("FILTER_NON_INDEXED_LT"),

    /**
     * The engine supports {@link com.stratio.crossdata.common.logicalplan.Filter} operations on columns
     * that are not indexed by the underlying datastore with greater than or equal operator.
     */
    FILTER_NON_INDEXED_GET("FILTER_NON_INDEXED_GET"),

    /**
     * The engine supports {@link com.stratio.crossdata.common.logicalplan.Filter} operations on columns
     * that are not indexed by the underlying datastore with less than or equal operator.
     */
    FILTER_NON_INDEXED_LET("FILTER_NON_INDEXED_LET"),

    /**
     * The engine supports {@link com.stratio.crossdata.common.logicalplan.Filter} operations on columns
     * that are not indexed by the underlying datastore with a distinct operator.
     */
    FILTER_NON_INDEXED_DISTINCT("FILTER_NON_INDEXED_DISTINCT"),

    /**
     * The engine supports full text search syntax in {@link com.stratio.crossdata.common.logicalplan.Filter}
     * operations with a MATCH Operator in non indexed columns.
     */
    FILTER_NON_INDEXED_MATCH("FILTER_NON_INDEXED_MATCH"),

    /**
     * The filter of like operator.
     */
    FILTER_NON_INDEXED_LIKE("FILTER_NON_INDEXED_LIKE"),

    /**
     * The engine supports {@link com.stratio.crossdata.common.logicalplan.Filter} operations on columns
     * that are not indexed by the underlying datastore with not like operator.
     */
    FILTER_NON_INDEXED_NOT_LIKE("FILTER_NON_INDEXED_NOT_LIKE"),

    /**
     * The engine supports {@link com.stratio.crossdata.common.logicalplan.Filter} operations on columns
     * that are not indexed by the underlying datastore with a IN operator for collections.
     */
    FILTER_NON_INDEXED_IN("FILTER_NON_INDEXED_IN"),

    /**
     * The engine supports {@link com.stratio.crossdata.common.logicalplan.Filter} operations on columns
     * that are not indexed by the underlying datastore with not in operator.
     */
    FILTER_NON_INDEXED_NOT_IN("FILTER_NON_INDEXED_NOT_IN"),

    /**
     * The engine supports {@link com.stratio.crossdata.common.logicalplan.Filter} operations on columns
     * that are not indexed by the underlying datastore with between operator.
     */
    FILTER_NON_INDEXED_BETWEEN("FILTER_NON_INDEXED_BETWEEN"),

    /**
     * The engine supports {@link com.stratio.crossdata.common.logicalplan.Filter} operations on columns
     * that are not indexed by the underlying datastore with not between operator.
     */
    FILTER_NON_INDEXED_NOT_BETWEEN("FILTER_NON_INDEXED_NOT_BETWEEN"),

    /**
     * The engine supports {@link com.stratio.crossdata.common.logicalplan.Filter} operations on columns
     * that have an associated index in the underlying datastore with an equal operator.
     */
    FILTER_INDEXED_EQ("FILTER_INDEXED_EQ"),

    /**
     * The engine supports {@link com.stratio.crossdata.common.logicalplan.Filter} operations on columns
     * that have an associated index in the underlying datastore with a greater than operator.
     */
    FILTER_INDEXED_GT("FILTER_INDEXED_GT"),

    /**
     * The engine supports {@link com.stratio.crossdata.common.logicalplan.Filter} operations on columns
     * that have an associated index in the underlying datastore with a less than operator.
     */
    FILTER_INDEXED_LT("FILTER_INDEXED_LT"),

    /**
     * The engine supports {@link com.stratio.crossdata.common.logicalplan.Filter} operations on columns
     * that have an associated index in the underlying datastore with a greater than or equal operator.
     */
    FILTER_INDEXED_GET("FILTER_INDEXED_GET"),

    /**
     * The engine supports {@link com.stratio.crossdata.common.logicalplan.Filter} operations on columns
     * that have an associated index in the underlying datastore with a less than or equal operator.
     */
    FILTER_INDEXED_LET("FILTER_INDEXED_LET"),

    /**
     * The engine supports {@link com.stratio.crossdata.common.logicalplan.Filter} operations on columns
     * that have an associated index in the underlying datastore with a distinct operator.
     */
    FILTER_INDEXED_DISTINCT("FILTER_INDEXED_DISTINCT"),

    /**
     * The engine supports full text search syntax in {@link com.stratio.crossdata.common.logicalplan.Filter}
     * operations with a MATCH Operator.
     */
    FILTER_INDEXED_MATCH("FILTER_INDEXED_MATCH"),

    /**
     * The filter of like operator over an index.
     */
    FILTER_INDEXED_LIKE("FILTER_INDEXED_LIKE"),

    /**
     * The engine supports {@link com.stratio.crossdata.common.logicalplan.Filter} operations on columns
     * that have an associated index in the underlying datastore with not like operator.
     */
    FILTER_INDEXED_NOT_LIKE("FILTER_INDEXED_NOT_LIKE"),

    /**
     * The engine supports {@link com.stratio.crossdata.common.logicalplan.Filter} operations on columns
     * that have an associated index in the underlying datastore with in operator.
     */
    FILTER_INDEXED_IN("FILTER_INDEXED_IN"),

    /**
     * The engine supports {@link com.stratio.crossdata.common.logicalplan.Filter} operations on columns
     * that have an associated index in the underlying datastore with not in operator.
     */
    FILTER_INDEXED_NOT_IN("FILTER_INDEXED_NOT_IN"),

    /**
     * The engine supports {@link com.stratio.crossdata.common.logicalplan.Filter} operations on columns
     * that have an associated index in the underlying datastore with between operator.
     */
    FILTER_INDEXED_BETWEEN("FILTER_INDEXED_BETWEEN"),

    /**
     * The engine supports {@link com.stratio.crossdata.common.logicalplan.Filter} operations on columns
     * that have an associated index in the underlying datastore with not between operator.
     */
    FILTER_INDEXED_NOT_BETWEEN("FILTER_INDEXED_NOT_BETWEEN"),

    /**
     * The engine supports {@link com.stratio.crossdata.common.logicalplan.Filter} operations using
     * a includes as part of a relation using with an equal operator.
     */
    FILTER_FUNCTION_EQ("FILTER_FUNCTION_EQ"),

    /**
     * The engine supports {@link com.stratio.crossdata.common.logicalplan.Filter} operations using
     * a includes as part of a relation using with a greater than operator.
     */
    FILTER_FUNCTION_GT("FILTER_FUNCTION_GT"),

    /**
     * The engine supports {@link com.stratio.crossdata.common.logicalplan.Filter} operations using
     * a includes as part of a relation using with a less than operator.
     */
    FILTER_FUNCTION_LT("FILTER_FUNCTION_LT"),

    /**
     * The engine supports {@link com.stratio.crossdata.common.logicalplan.Filter} operations using
     * a includes as part of a relation using with a greater than or equal operator.
     */
    FILTER_FUNCTION_GET("FILTER_FUNCTION_GET"),

    /**
     * The engine supports {@link com.stratio.crossdata.common.logicalplan.Filter} operations using
     * a includes as part of a relation using with a less than or equal operator.
     */
    FILTER_FUNCTION_LET("FILTER_FUNCTION_LET"),

    /**
     * The engine supports {@link com.stratio.crossdata.common.logicalplan.Filter} operations using
     * a includes as part of a relation using with a distinct operator.
     */
    FILTER_FUNCTION_DISTINCT("FILTER_FUNCTION_DISTINCT"),

    /**
     * The filter of like operator.
     */
    FILTER_FUNCTION_LIKE("FILTER_FUNCTION_LIKE"),

    /**
     * The engine supports {@link com.stratio.crossdata.common.logicalplan.Filter} operations using
     * a includes as part of a relation using with not like operator.
     */
    FILTER_FUNCTION_NOT_LIKE("FILTER_FUNCTION_NOT_LIKE"),

    /**
     * The engine supports {@link com.stratio.crossdata.common.logicalplan.Filter} operations using
     * a includes as part of a relation using with in operator.
     */
    FILTER_FUNCTION_IN("FILTER_FUNCTION_IN"),

    /**
     * The engine supports {@link com.stratio.crossdata.common.logicalplan.Filter} operations using
     * a includes as part of a relation using with not in operator.
     */
    FILTER_FUNCTION_NOT_IN("FILTER_FUNCTION_NOT_IN"),

    /**
     * The engine supports {@link com.stratio.crossdata.common.logicalplan.Filter} operations using
     * a includes as part of a relation using with between operator.
     */
    FILTER_FUNCTION_BETWEEEN("FILTER_FUNCTION_BETWEEEN"),

    /**
     * The engine supports {@link com.stratio.crossdata.common.logicalplan.Filter} operations using
     * a includes as part of a relation using with not between operator.
     */
    FILTER_FUNCTION_NOT_BETWEEEN("FILTER_FUNCTION_NOT_BETWEEEN"),

    /**
     * The engine supports OR operator.
     */
    FILTER_DISJUNCTION("FILTER_DISJUNCTION"),

    /**
     * The engine supports {@link com.stratio.crossdata.common.logicalplan.PartialResults} operations
     * to read a list of partial results.
     */
    PARTIAL_RESULTS("PARTIAL_RESULTS"),

    /**
     * The engine supports chunking the result of a Select query.
     */
    PAGINATION("PAGINATION"),

    /**
     * The engine supports direct execution of SQL queries.
     */
    SQL_DIRECT("SQL_DIRECT"),

    /**
     * The engine support insert into from select queries.
     */
    INSERT_FROM_SELECT("INSERT_FROM_SELECT"),

    /**
     * The engine supports left outer joins.
     */
    SELECT_LEFT_OUTER_JOIN("SELECT_LEFT_OUTER_JOIN"),

    /**
     * The engine supports right outer joins.
     */
    SELECT_RIGHT_OUTER_JOIN("SELECT_OUTER_RIGHT_JOIN"),

    /**
     * The engine supports FULL outer joins.
     */
    SELECT_FULL_OUTER_JOIN("SELECT_FULL_OUTER_JOIN"),

    /**
     * The engine supports natural joins.
     */
    SELECT_FULL_NATURAL_JOIN("SELECT_NATURAL_JOIN"),

    /**
     * The engine supports cross joins.
     */
    SELECT_CROSS_JOIN("SELECT_CROSS_JOIN"),

    /**
     * The engine supports left outer joins with partial results.
     */
    SELECT_LEFT_OUTER_JOIN_PARTIALS_RESULTS("SELECT_LEFT_OUTER_JOIN_PARTIALS_RESULTS"),

    /**
     * The engine supports right outer joins with partial results.
     */
    SELECT_RIGHT_OUTER_JOIN_PARTIALS_RESULTS("SELECT_RIGHT_OUTER_JOIN_PARTIALS_RESULTS"),

    /**
     * The engine supports full outer joins with partial results.
     */
    SELECT_FULL_OUTER_JOIN_PARTIALS_RESULTS("SELECT_FULL_OUTER_JOIN_PARTIALS_RESULTS"),

    /**
     * The engine supports cross joins with partial results.
     */
    SELECT_CROSS_JOIN_PARTIALS_RESULTS("SELECT_CROSS_JOIN_PARTIALS_RESULTS"),

    /**
     * The engine supports operations which refer to the result of previous queries by using aliases.
     */
    SELECT_SUBQUERY("SELECT_SUBQUERY");


    private String operationsStr;

    Operations(String operationsStr) {
        this.operationsStr = operationsStr;
    }

    public String getOperationsStr() {
        return operationsStr;
    }

}
