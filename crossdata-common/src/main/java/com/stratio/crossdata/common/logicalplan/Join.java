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

package com.stratio.crossdata.common.logicalplan;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import com.stratio.crossdata.common.data.JoinType;
import com.stratio.crossdata.common.metadata.Operations;
import com.stratio.crossdata.common.statements.structures.Relation;

/**
 * Join operator.
 */
public class Join extends UnionStep {

    private static final long serialVersionUID = -5886407607386975852L;
    /**
     * Join identifier.
     */
    private final String id;

    /**
     *
     * Join type.
     */
    private JoinType type;

    /**
     * List of logical step identifiers involved in the join.
     */
    private final List<String> sourceIdentifiers = new ArrayList<>();

    /**
     * List of join relations.
     */
    private final List<Relation> joinRelations = new ArrayList<>();

    /**
     * Class constructor.
     *
     * @param operations The operations to be applied.
     * @param id        The join identifier.
     */
    public Join(Set<Operations> operations, String id) {
        super(operations);
        this.id = id;
    }

    /**
     * Add a Relation in the Join.
     * @param r The relation
     */
    public void addJoinRelation(Relation r) {
        joinRelations.add(r);
    }

    /**
     * Add a list of relations in the Join.
     * @param list The list of Relation
     */
    public void addJoinRelations(List<Relation> list) {
        joinRelations.addAll(list);
    }

    /**
     * Get the Relations of the join.
     * @return List of logical step identifiers involved in the join.
     */
    public List<Relation> getJoinRelations() {
        return joinRelations;
    }

    /**
     * Add an identifier.
     * @param id The identifier
     */
    public void addSourceIdentifier(String id) {
        sourceIdentifiers.add(id);
    }

    /**
     * Add a list of identifiers (name of the tables implied) of the join.
     * @param ids List of identifiers.
     */
    public void addSourceIdentifier(List<String> ids) {
        sourceIdentifiers.addAll(ids);
    }

    /**
     * Get the Identifiers.
     * @return List of logical step identifiers involved in the join.
     */
    public List<String> getSourceIdentifiers() {
        return sourceIdentifiers;
    }

    /**
     * Get the id.
     * @return Join identifier.
     */
    public String getId() {
        return id;
    }

    /**
     * Get the type.
     * @return a {@link com.stratio.crossdata.common.data.JoinType} .
     */
    public JoinType getType() {
        return type;
    }

    /**
     * Set the type.
     * @param type The type.
     */
    public void setType(JoinType type) {
        this.type = type;
    }

    @Override
    public String toString() {
        //StringBuilder sb = new StringBuilder(type.toString());
        StringBuilder sb = new StringBuilder();
        if(type != null){
            sb.append(type.toString());
        } else {
            sb.append("INNER ");
        }
        sb.append(" JOIN (");
        sb.append(sourceIdentifiers.toString());
        if(type != JoinType.CROSS){
            sb.append(") ON ").append(joinRelations.toString());
        }
        return sb.toString();
    }
}
