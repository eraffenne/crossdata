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

package com.stratio.crossdata.core.planner;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.google.common.collect.Sets;
import com.stratio.crossdata.common.metadata.*;
import com.stratio.crossdata.common.statements.structures.*;
import com.stratio.crossdata.core.planner.utils.ConnectorPriorityComparator;
import org.apache.log4j.Logger;

import com.stratio.crossdata.common.data.AlterOperation;
import com.stratio.crossdata.common.data.AlterOptions;
import com.stratio.crossdata.common.data.CatalogName;
import com.stratio.crossdata.common.data.Cell;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.data.ConnectorName;
import com.stratio.crossdata.common.data.IndexName;
import com.stratio.crossdata.common.data.JoinType;
import com.stratio.crossdata.common.data.Row;
import com.stratio.crossdata.common.data.Status;
import com.stratio.crossdata.common.data.TableName;
import com.stratio.crossdata.common.exceptions.IgnoreQueryException;
import com.stratio.crossdata.common.exceptions.PlanningException;
import com.stratio.crossdata.common.exceptions.ValidationException;
import com.stratio.crossdata.common.executionplan.*;
import com.stratio.crossdata.common.logicalplan.*;
import com.stratio.crossdata.common.manifest.FunctionType;
import com.stratio.crossdata.common.utils.Constants;
import com.stratio.crossdata.common.utils.StringUtils;
import com.stratio.crossdata.core.metadata.MetadataManager;
import com.stratio.crossdata.core.query.*;
import com.stratio.crossdata.core.statements.*;
import com.stratio.crossdata.core.structures.ExtendedSelectSelector;
import com.stratio.crossdata.core.structures.Join;
import com.stratio.crossdata.core.utils.CoreUtils;
import com.stratio.crossdata.core.validator.Validator;

/**
 * Class in charge of defining the set of {@link com.stratio.crossdata.common.logicalplan.LogicalStep}
 * required to execute a statement. This set of steps are ordered as a workflow on a {@link
 * com.stratio.crossdata.common.logicalplan.LogicalWorkflow} structure. Notice that the LogicalWorkflow
 * may contain several initial steps, but it will always finish in a single operation.
 */
public class Planner {

    /**
     * Class logger.
     */
    private static final Logger LOG = Logger.getLogger(Planner.class);
    private final String host;

    /**
     * Constructor class based in the Host.
     * @param host the host used to get the AKKA actor reference.
     */
    public Planner(String host) {
        this.host = host;
    }

    public String getHost() {
        return host;
    }

    /**
     * Define a logical workflow that represents the operations required for executing the {@code SELECT} query sent
     * by the user. After that, an ExecutionWorkflow is creating determining which connectors will execute the
     * different elements of the original query.
     *
     * @param query A {@link com.stratio.crossdata.core.query.SelectValidatedQuery}.
     * @return A {@link com.stratio.crossdata.core.query.SelectPlannedQuery}.
     * @throws com.stratio.crossdata.common.exceptions.PlanningException If the query cannot be planned.
     */
    public SelectPlannedQuery planQuery(SelectValidatedQuery query) throws PlanningException {
        query.optimizeQuery();
        LogicalWorkflow workflow = buildWorkflow(query);
        //Plan the workflow execution into different connectors.
        ExecutionWorkflow executionWorkflow = buildExecutionWorkflow(query, workflow);
        //Return the planned query.
        SelectPlannedQuery plannedQuery = new SelectPlannedQuery(query, executionWorkflow);
        //Add the sql direct query to the logical workflow of the last executionWorkflow.
        plannedQuery.getLastLogicalWorkflow().setSqlDirectQuery(query.getStatement().toSQL92String());
        LOG.info("SQL Direct: " + ((QueryWorkflow) plannedQuery.getExecutionWorkflow()).getWorkflow().getSqlDirectQuery());
        plannedQuery.getLastLogicalWorkflow().setOptions(query.getStatement().getProperties());
        return plannedQuery;
    }

    /**
     * Define an execution plan for metadata queries.
     *
     * @param query A {@link com.stratio.crossdata.core.query.MetadataValidatedQuery}.
     * @return A {@link com.stratio.crossdata.core.query.MetadataPlannedQuery}.
     * @throws PlanningException If the query cannot be planned.
     */
    public MetadataPlannedQuery planQuery(MetadataValidatedQuery query) throws PlanningException {
        ExecutionWorkflow executionWorkflow = buildExecutionWorkflow(query);
        return new MetadataPlannedQuery(query, executionWorkflow);
    }

    /**
     * Define an execution plan for storage queries.
     *
     * @param query A {@link com.stratio.crossdata.core.query.StorageValidatedQuery}.
     * @return A {@link com.stratio.crossdata.core.query.StoragePlannedQuery}.
     * @throws PlanningException If the query cannot be planned.
     */
    public StoragePlannedQuery planQuery(StorageValidatedQuery query) throws PlanningException {
        ExecutionWorkflow executionWorkflow = buildExecutionWorkflow(query);
        return new StoragePlannedQuery(query, executionWorkflow);
    }

    /**
     * Build a execution workflow for a query analyzing the existing logical workflow.
     *
     * @param query  The {@link com.stratio.crossdata.core.query.SelectValidatedQuery}.
     * @param workflow The {@link com.stratio.crossdata.common.logicalplan.LogicalWorkflow} associated with the query.
     * @return A {@link com.stratio.crossdata.common.executionplan.ExecutionWorkflow}.
     * @throws PlanningException If the workflow cannot be defined.
     */
    protected ExecutionWorkflow buildExecutionWorkflow(SelectValidatedQuery query, LogicalWorkflow workflow)
            throws PlanningException {

        List<ConnectorMetadata> connectedConnectors = MetadataManager.MANAGER.getConnectors(Status.ONLINE);

        if((connectedConnectors == null) || (connectedConnectors.isEmpty())){
            throw new PlanningException("There are no connectors online");
        }

        //Get the list of tables accessed in this query
        List<TableName> tables = getInitialSteps(workflow.getInitialSteps());

        //Obtain the map of connector that is able to access those tables.
        Map<TableName, List<ConnectorMetadata>> candidatesConnectors = MetadataManager.MANAGER
                .getAttachedConnectors(Status.ONLINE, tables);

        logCandidateConnectors(candidatesConnectors);

        Set<ExecutionPath> executionPaths = new LinkedHashSet<>();
        Map<UnionStep, LinkedHashSet<ExecutionPath>> unionSteps = new LinkedHashMap<>();
        //Iterate through the initial steps and build valid execution paths
        for (LogicalStep initialStep: workflow.getInitialSteps()) {
            TableName targetTable = ((Project) initialStep).getTableName();
            LOG.info("Table: " + targetTable);
            // Detect step before the next union step from the current step
            ExecutionPath ep = new ExecutionPath(
                    initialStep,
                    initialStep,
                    candidatesConnectors.get(targetTable));
            while(hasMoreUnionSteps(ep.getInitial())){
                ep = defineExecutionPath(ep.getLast(), candidatesConnectors.get(targetTable), query);
                LOG.info("Last step: " + ep.getLast());
                if (ep.getLast().getNextStep() != null && UnionStep.class.isInstance(ep.getLast().getNextStep())) {
                    LinkedHashSet<ExecutionPath> paths = unionSteps.get(ep.getLast().getNextStep());
                    if (paths == null) {
                        paths = new LinkedHashSet<>();
                        unionSteps.put((UnionStep) (ep.getLast().getNextStep()), paths);
                    }
                    paths.add(ep);
                }

                executionPaths.add(ep);
                if(ep.getLast().getNextStep() == null){
                    break;
                }
                ep = new ExecutionPath(
                        ep.getLast().getNextStep(),
                        ep.getLast().getNextStep(),
                        new ArrayList<ConnectorMetadata>());

            }
        }
        // Add ExecutionPath from last union step, if present; otherwise, add the whole path
        LogicalStep initialStep = workflow.getInitialSteps().get(0);
        LogicalStep lastUnion = getLastUnion(initialStep);
        List<ConnectorMetadata> connectorsForLastStep = MetadataManager.MANAGER.getConnectors(Status.ONLINE);
        if(initialStep.equals(lastUnion)){
            connectorsForLastStep = candidatesConnectors.get(((Project) initialStep).getTableName());
        }
        ExecutionPath lastExecPath = defineExecutionPath(
                lastUnion,
                connectorsForLastStep,
                query);
        executionPaths.add(lastExecPath);

        for (ExecutionPath ep: executionPaths) {
            LOG.info("ExecutionPaths: " + ep);
        }
        //Merge execution paths
        return mergeExecutionPaths(query, new ArrayList<>(executionPaths), unionSteps);
    }

    private LogicalStep getLastUnion(LogicalStep logicalStep) {
        LogicalStep lastUnion = logicalStep;
        while(hasMoreUnionSteps(lastUnion)){
            lastUnion = getNextUnionStep(lastUnion.getNextStep());
        }
        return lastUnion;
    }

    private LogicalStep getNextUnionStep(LogicalStep lastUnion) {
        while(!(lastUnion instanceof UnionStep)){
            lastUnion = lastUnion.getNextStep();
        }
        return lastUnion;
    }

    protected ExecutionWorkflow buildSimpleExecutionWorkflow(SelectValidatedQuery query, LogicalWorkflow workflow,
            ConnectorMetadata connectorMetadata)
            throws PlanningException {

        Set<Operations> supportedOperations = connectorMetadata.getSupportedOperations();

        for (LogicalStep step : workflow.getInitialSteps()) {
            //Iterate path
            LogicalStep currentStep = step;
            do {
                for (Operations operation : currentStep.getOperations()) {
                    if (!supportedOperations.contains(operation)){
                        throw new PlanningException("The connector "+connectorMetadata.getName()+" does not support "+operation);
                    }
                }
                currentStep = currentStep.getNextStep();
            }while(currentStep!= null);

        }

        String queryId = query.getQueryId();
        ExecutionType executionType = ExecutionType.SELECT;
        ResultType type = ResultType.RESULTS;

        if ((supportedOperations.contains(Operations.PAGINATION)) && (connectorMetadata.getPageSize() > 0)) {
            workflow.setPagination(connectorMetadata.getPageSize());
        }

        return new QueryWorkflow(queryId, connectorMetadata.getActorRef(host), executionType, type, workflow,
                supportedOperations.contains(Operations.ASYNC_QUERY));
    }


    private boolean hasMoreUnionSteps(LogicalStep step) {
        boolean result = false;
        while(step.getNextStep() != null){
            step = step.getNextStep();
            if(step instanceof com.stratio.crossdata.common.logicalplan.Join){
                result = true;
                break;
            }
        }
        return result;
    }

    private void logCandidateConnectors(Map<TableName, List<ConnectorMetadata>> candidatesConnectors) {
        StringBuilder sb = new StringBuilder("Candidate connectors: ").append(System.lineSeparator());
        for (Map.Entry<TableName, List<ConnectorMetadata>> tableEntry: candidatesConnectors.entrySet()) {
            for (ConnectorMetadata cm: tableEntry.getValue()) {
                sb.append("\ttable: ").append(tableEntry.getKey().toString()).append(" ").append(cm.getName())
                        .append(" ").append(cm.getActorRef(host)).append(System.lineSeparator());
            }
        }
        LOG.info(sb.toString());
    }

    /**
     * Merge a set of execution paths solving union dependencies along.
     *
     * @param svq            The {@link com.stratio.crossdata.core.query.SelectValidatedQuery}.
     * @param executionPaths The list of execution paths.
     * @param unionSteps     A map of union steps waiting to be merged.
     * @return A {@link com.stratio.crossdata.common.executionplan.ExecutionWorkflow}.
     * @throws PlanningException If the execution paths cannot be merged.
     */
    protected ExecutionWorkflow mergeExecutionPaths(SelectValidatedQuery svq, List<ExecutionPath> executionPaths,
            Map<UnionStep, LinkedHashSet<ExecutionPath>> unionSteps) throws PlanningException {

        String queryId = svq.getQueryId();

        if (unionSteps.size() == 0) {
            return toExecutionWorkflow(queryId, executionPaths, executionPaths.get(0).getLast(),
                    executionPaths.get(0).getAvailableConnectors(), ResultType.RESULTS);
        }
        LOG.info("UnionSteps: " + unionSteps.toString());
        //Find first UnionStep
        Set<UnionStep> mergeSteps = new LinkedHashSet<>();
        Map<UnionStep, ExecutionPath[]> pathsMap = new LinkedHashMap<>();
        ExecutionPath[] paths;
        QueryWorkflow first = null;
        Map<PartialResults, ExecutionWorkflow> triggerResults = new LinkedHashMap<>();
        Map<UnionStep, ExecutionWorkflow> triggerWorkflow = new LinkedHashMap<>();

        addPathsAndMergeSteps(unionSteps, mergeSteps, pathsMap);
        List<UnionStep> toConvertToPartial = new ArrayList<>();

        for(UnionStep mergeStep: mergeSteps) {
            Set<ConnectorMetadata> toRemove = new LinkedHashSet<>();
            Set<ConnectorMetadata> mergeConnectors = new LinkedHashSet<>();
            Set<ExecutionWorkflow> workflows = new LinkedHashSet<>();
            Set<ExecutionPath> toMerge = new LinkedHashSet<>();
            boolean[] intermediateResults = new boolean[2];
            //Check whether the list of connectors found in the Execution paths being merged can execute the join
            intermediateResults[0] = false;
            intermediateResults[1] = false;
            mergeConnectors.clear();
            toMerge.clear();
            ExecutionPath[] mergePaths = pathsMap.get(mergeStep);
            for (int index = 0; index < mergePaths.length; index++) {
                toRemove.clear();
                getToRemove(mergeStep, toRemove, mergePaths[index]);

                if (mergePaths[index].getAvailableConnectors().size() == toRemove.size()) {
                    //Add intermediate result node
                    PartialResults partialResults = addIntermediateNode(mergeStep, mergePaths[index]);

                    //Create a trigger execution workflow with the partial results step.
                    ExecutionWorkflow w = toExecutionWorkflow(queryId, Arrays.asList(mergePaths[index]),
                            mergePaths[index].getLast(), mergePaths[index].getAvailableConnectors(),
                            ResultType.TRIGGER_EXECUTION);
                    w.setTriggerStep(partialResults);

                    triggerResults.put(partialResults, w);

                    // Add select step for connector
                    first = addSelectStep(first, workflows, intermediateResults, mergePaths[index], index, w);

                    //Merge Step that contains a *_JOIN has to be changed to *_JOIN_PARTIAL_RESULTS
                    toConvertToPartial.add(mergeStep);
                } else {
                    mergePaths[index].getAvailableConnectors().removeAll(toRemove);
                    //Add to the list of mergeConnectors.
                    mergeConnectors.addAll(mergePaths[index].getAvailableConnectors());
                    toMerge.add(mergePaths[index]);
                }
            }

            ExecutionPath next = defineExecutionPath(
                    mergeStep,
                    new ArrayList<>(mergeConnectors),
                    svq);

            if (Select.class.isInstance(next.getLast())) {
                ExecutionWorkflow mergeWorkflow = extendExecutionWorkflow(
                        queryId,
                        new ArrayList<>(toMerge),
                        next,
                        ResultType.RESULTS);
                triggerWorkflow.put(mergeStep, mergeWorkflow);
                if (first == null) {
                    first = QueryWorkflow.class.cast(mergeWorkflow);
                }
            } else {
                linkPathsToUnionStep(new ArrayList<>(toMerge), next);
                LinkedHashSet<ExecutionPath> existingPaths = unionSteps.get(next.getLast());
                if (executionPaths == null) {
                    existingPaths = new LinkedHashSet<>();
                } else {
                    executionPaths.add(next);
                }
                if(next.getLast() instanceof UnionStep){
                    unionSteps.put(UnionStep.class.cast(next.getLast()), existingPaths);
                }
            }
        }

        convertToPartial(toConvertToPartial);

        return buildExecutionTree(first, triggerResults, triggerWorkflow);
    }

    /**
     * Convert to any type of join to join partial result if it is contained in the list toConvertToPartialResult.
     * @param toConvertToPartial
     */
    private void convertToPartial(List<UnionStep> toConvertToPartial) {
        for(int i=0; i<toConvertToPartial.size(); i++){
            com.stratio.crossdata.common.logicalplan.Join partialJoin =
                    (com.stratio.crossdata.common.logicalplan.Join) toConvertToPartial.get(i);
            partialJoin.setId(partialJoin.getId()+"PartialResults");
            Set<Operations> operations = new HashSet<>(partialJoin.getOperations());
            partialJoin.removeAllOperations();
            for(Operations operation: operations){
                String operationString = operation.getOperationsStr();
                if(operationString.toUpperCase().contains("JOIN")){
                    operationString = operationString + "_PARTIAL_RESULTS";
                }
                partialJoin.addOperation(Operations.valueOf(operationString));
            }
        }
    }

    private QueryWorkflow addSelectStep(QueryWorkflow first, Set<ExecutionWorkflow> workflows,
            boolean[] intermediateResults, ExecutionPath mergePath, int index, ExecutionWorkflow w) {
        Project project = (Project) mergePath.getInitial();
        Select previousSelect = generateSelectFromProject(project);
        QueryWorkflow qw = (QueryWorkflow) w;
        LogicalStep lastStep = qw.getWorkflow().getLastStep();
        lastStep.setNextStep(previousSelect);
        previousSelect.setPrevious(lastStep);
        qw.getWorkflow().setLastStep(previousSelect);

        workflows.add(w);
        intermediateResults[index] = true;
        if (first == null) {
            first = QueryWorkflow.class.cast(w);
        }
        return first;
    }

    private PartialResults addIntermediateNode(UnionStep finalMergeStep, ExecutionPath mergePath) {
        PartialResults partialResults = new PartialResults(
                Collections.singleton(Operations.PARTIAL_RESULTS));
        partialResults.setNextStep(finalMergeStep);

        finalMergeStep.addPreviousSteps(partialResults);
        finalMergeStep.removePreviousStep(mergePath.getLast());
        mergePath.getLast().setNextStep(null);
        return partialResults;
    }

    private void getToRemove(UnionStep mergeStep, Set<ConnectorMetadata> toRemove, ExecutionPath mergePath) {
        for (ConnectorMetadata connector: mergePath.getAvailableConnectors()) {
            LOG.info("op: " + mergeStep.getOperations() + " -> " +
                    connector.supports(mergeStep.getOperations()));
            if (!connector.supports(mergeStep.getOperations())) {
                toRemove.add(connector);
            }
        }
    }

    private void addPathsAndMergeSteps(Map<UnionStep, LinkedHashSet<ExecutionPath>> unionSteps, Set<UnionStep> mergeSteps, Map<UnionStep, ExecutionPath[]> pathsMap) {
        ExecutionPath[] paths;
        for (Map.Entry<UnionStep, LinkedHashSet<ExecutionPath>> entry: unionSteps.entrySet()) {
            paths = entry.getValue().toArray(new ExecutionPath[entry.getValue().size()]);
            pathsMap.put(entry.getKey(), paths);
            mergeSteps.add(entry.getKey());
        }
    }

    private Select generateSelectFromProject(Project project) {
        Set<Operations> requiredOperations = Collections.singleton(Operations.SELECT_OPERATOR);
        Map<Selector, String> columnMap = new LinkedHashMap<>();
        Map<String, ColumnType> typeMap = new LinkedHashMap<>();
        Map<Selector, ColumnType> typeMapFromColumnName = new LinkedHashMap<>();
        for(ColumnName cn: project.getColumnList()){
            ColumnMetadata columnMetadata = MetadataManager.MANAGER.getColumn(cn);
            ColumnType ct = columnMetadata.getColumnType();
            if(columnMetadata.getName().getAlias() == null){
                columnMetadata.getName().setAlias(columnMetadata.getName().getName());
            }
            String alias = columnMetadata.getName().getAlias();
            ColumnSelector cs = new ColumnSelector(columnMetadata.getName());
            cs.setAlias(alias);
            columnMap.put(cs, alias);
            typeMap.put(alias, ct);
            typeMapFromColumnName.put(cs, ct);
        }
        return new Select(requiredOperations, columnMap, typeMap, typeMapFromColumnName);
    }

    /**
     * Build the tree of linked execution workflows.
     *
     * @param first            The first workflow of the list.
     * @param triggerResults   The map of triggering steps.
     * @param triggerWorkflows The map of workflows associated with merge steps.
     * @return A {@link com.stratio.crossdata.common.executionplan.ExecutionWorkflow}.
     */
    public ExecutionWorkflow buildExecutionTree(QueryWorkflow first,
            Map<PartialResults, ExecutionWorkflow> triggerResults,
            Map<UnionStep, ExecutionWorkflow> triggerWorkflows) {

        LogicalStep triggerStep = first.getTriggerStep();
        ExecutionWorkflow workflow = first;

        while (!triggerResults.isEmpty()) {
            workflow.setNextExecutionWorkflow(triggerWorkflows.get(triggerStep.getNextStep()));
            triggerResults.remove(triggerStep);
            workflow = workflow.getNextExecutionWorkflow();
            triggerStep = workflow.getTriggerStep();
        }

        return first;
    }

    /**
     * Define a query workflow.
     *
     * @param queryId        The query identifier.
     * @param executionPaths The list of execution paths that will be transformed into initial steps of a
     *                       {@link com.stratio.crossdata.common.logicalplan.LogicalWorkflow}.
     * @param last           The last element of the workflow.
     * @param connectors     The List of available connectors.
     * @return A {@link com.stratio.crossdata.common.executionplan.QueryWorkflow}.
     */
    protected QueryWorkflow toExecutionWorkflow(String queryId, List<ExecutionPath> executionPaths, LogicalStep last,
            List<ConnectorMetadata> connectors, ResultType type) throws PlanningException {

        //Define the list of initial steps.
        List<LogicalStep> initialSteps = new ArrayList<>(executionPaths.size());
        List<ClusterName> involvedClusters = new ArrayList<>(executionPaths.size());

        for (ExecutionPath path: executionPaths) {
            Project project = (Project) path.getInitial();
            involvedClusters.add(project.getClusterName());
            initialSteps.add(path.getInitial());
        }
        LogicalWorkflow workflow = new LogicalWorkflow(initialSteps);

        //Select an actor
        Iterator<ConnectorMetadata> connectorMetadata = getConnectorsSortedByPriority(connectors, involvedClusters).iterator();

        QueryWorkflow choicedWorkflow = buildQueryWorkflowFromConnector(queryId, connectorMetadata.next(), ExecutionType.SELECT, type, workflow);
        QueryWorkflow tempWorkflow = choicedWorkflow;
        while(connectorMetadata.hasNext()){
            QueryWorkflow auxWorkflow = buildQueryWorkflowFromConnector(queryId, connectorMetadata.next(),
                            ExecutionType.SELECT, type, workflow);
            tempWorkflow.setLowPriorityExecutionWorkflow(auxWorkflow);
            tempWorkflow = auxWorkflow;
        }

        return choicedWorkflow;
    }

    private QueryWorkflow buildQueryWorkflowFromConnector(String queryId, ConnectorMetadata connectorMetadata, ExecutionType exType, ResultType resType, LogicalWorkflow workflow) throws PlanningException {

        String selectedActorUri = StringUtils.getAkkaActorRefUri(connectorMetadata.getActorRef(host), false);
        LogicalWorkflow logicalWorkflow = new LogicalWorkflow(workflow);
        updateFunctionsFromSelect(logicalWorkflow, connectorMetadata.getName());

        if ((connectorMetadata.getSupportedOperations().contains(Operations.PAGINATION)) && (
                connectorMetadata.getPageSize() > 0)) {
            logicalWorkflow.setPagination(connectorMetadata.getPageSize());
        }
        return new QueryWorkflow(queryId, selectedActorUri, exType, resType, logicalWorkflow, connectorMetadata.getSupportedOperations().contains(Operations.ASYNC_QUERY));
    }




    private List<ConnectorMetadata> getConnectorsSortedByPriority(List<ConnectorMetadata> connectors, List<ClusterName> clusters) {
        //TODO: Add logic to this method according to native or not
        //TODO: Add logic to this method according to statistics

        if ( connectors.size() > 1){
            Collections.sort(connectors, new ConnectorPriorityComparator(clusters));
            if(LOG.isDebugEnabled()){
                List<ConnectorName> connectorNames = new ArrayList();
                for (ConnectorMetadata connector : connectors) {
                    connectorNames.add(connector.getName());
                }
                LOG.debug("ConnectorList sorted by priority " + StringUtils.stringList(connectorNames, " , "));
            }
        }

        return connectors;
    }

    private void updateFunctionsFromSelect(LogicalWorkflow workflow, ConnectorName connectorName)
            throws PlanningException {

        if (!Select.class.isInstance(workflow.getLastStep())) {
            return;
        }

        Select selectStep = Select.class.cast(workflow.getLastStep());

        Map<String, ColumnType> typeMap = selectStep.getTypeMap();

        Map<Selector, ColumnType> typeMapFromColumnName = selectStep.getTypeMapFromColumnName();

        for (Selector s: typeMapFromColumnName.keySet()) {
            if (FunctionSelector.class.isInstance(s)) {
                FunctionSelector fs = FunctionSelector.class.cast(s);
                String functionName = fs.getFunctionName();
                FunctionType ft = MetadataManager.MANAGER.getFunction(connectorName, functionName, getSetOfProjects(workflow.getInitialSteps()));
                if(ft == null){
                    throw new PlanningException("Function: '" + functionName + "' unrecognized");
                }
                String returningType = StringUtils.getReturningTypeFromSignature(ft.getSignature());
                ColumnType ct = StringUtils.convertXdTypeToColumnType(returningType);
                typeMapFromColumnName.put(fs, ct);
                String stringKey = fs.getFunctionName();
                if (fs.getAlias() != null) {
                    stringKey = fs.getAlias();
                }
                typeMap.put(stringKey, ct);
            }
        }
    }

    //TODO Use generics.
    private Set<Project> getSetOfProjects(List<LogicalStep> initialSteps){
        Set<Project> initialProjects = new HashSet<>();
        for (LogicalStep initialStep : initialSteps) {
            initialProjects.add((Project) initialStep);
        }
        return initialProjects;
    }

    private void linkPathsToUnionStep(List<ExecutionPath> executionPaths,
            ExecutionPath mergePath){
        for (ExecutionPath path: executionPaths) {
            path.getLast().setNextStep(mergePath.getInitial()); //TODO; Previous?
        }
    }

    /**
     * Define a query workflow composed by several execution paths merging in a
     * {@link com.stratio.crossdata.common.executionplan.ExecutionPath} that starts with a UnionStep.
     *
     * @param queryId        The query identifier.
     * @param executionPaths The list of execution paths.
     * @param mergePath      The merge path with the union step.
     * @param type           The type of results to be returned.
     * @return A {@link com.stratio.crossdata.common.executionplan.QueryWorkflow}.
     */
    protected QueryWorkflow extendExecutionWorkflow(String queryId, List<ExecutionPath> executionPaths,
            ExecutionPath mergePath, ResultType type) throws PlanningException {

        //Define the list of initial steps.
        List<LogicalStep> initialSteps = new ArrayList<>(executionPaths.size());
        List<ClusterName> involvedClusters = new ArrayList<>(executionPaths.size());

        for (ExecutionPath path: executionPaths) {
            Set<Project> previousInitialProjects = findPreviousInitialProjects(path.getInitial());
            initialSteps.addAll(previousInitialProjects);
            path.getLast().setNextStep(mergePath.getInitial()); //TODO; Previous?
        }

        for (LogicalStep previousProject : initialSteps) {
            involvedClusters.add(((Project) previousProject).getClusterName());
        }

        LogicalWorkflow workflow = new LogicalWorkflow(initialSteps);

        //Select an actor
        Iterator<ConnectorMetadata> connectorMetadata = getConnectorsSortedByPriority(mergePath.getAvailableConnectors(), involvedClusters).iterator();

        QueryWorkflow choicedWorkflow = buildQueryWorkflowFromConnector(queryId, connectorMetadata.next(), ExecutionType.SELECT, type, workflow);
        QueryWorkflow tempWorkflow = choicedWorkflow;
        while (connectorMetadata.hasNext()) {

            QueryWorkflow auxWorkflow = buildQueryWorkflowFromConnector(queryId, connectorMetadata.next(),
                            ExecutionType.SELECT, type, workflow);
            tempWorkflow.setLowPriorityExecutionWorkflow(auxWorkflow);
            tempWorkflow = auxWorkflow;

        }

        return choicedWorkflow;
    }


    private Set<Project> findPreviousInitialProjects(LogicalStep initialStep){
        Set<Project> initialProjects = new HashSet<>();

        if(initialStep instanceof Project){
            Project project = (Project) initialStep;
            initialProjects.add(project);
        } else {
            //Retrieve the unionStep
            while(initialStep instanceof TransformationStep){
                initialStep = initialStep.getFirstPrevious();
            }
            UnionStep us = (UnionStep) initialStep;
            Set<Project> firstSteps = findFirstSteps(us);
            initialProjects.addAll(firstSteps);
        }
        return initialProjects;
    }


    private Set<Project> findFirstSteps(UnionStep unionStep) {
        Set<Project> projects = new LinkedHashSet<>();
        for(LogicalStep ls: unionStep.getPreviousSteps()){
            if(UnionStep.class.isInstance(ls)){
                UnionStep us = (UnionStep) ls;
                projects.addAll(findFirstSteps(us));
            } else if(Project.class.isInstance(ls)) {
                Project p = (Project) ls;
                if(!p.isVirtual()){
                    projects.add(p);
                }
            } else {
                LogicalStep current = ls;
                while((!UnionStep.class.isInstance(current)) && (!Project.class.isInstance(current))){
                    current = current.getFirstPrevious();
                }
                if(current instanceof UnionStep){
                    UnionStep currentUnionStep = (UnionStep) current;
                    projects.addAll(findFirstSteps(currentUnionStep));
                } else if(current instanceof Project) {
                    Project currentProject = (Project) current;
                    projects.add(currentProject);
                }
            }
        }
        return projects;
    }

    /**
     * Define the a execution path that starts with a initial step. This process refines the list of available
     * connectors in order to obtain the list that supports all operations in an execution paths.
     *
     * @param initial             The initial step.
     * @param candidateConnectors The list of available connectors.
     * @return An {@link com.stratio.crossdata.common.executionplan.ExecutionPath}.
     * @throws PlanningException If the execution path cannot be determined.
     */
    protected ExecutionPath defineExecutionPath(LogicalStep initial, List<ConnectorMetadata> candidateConnectors,
            SelectValidatedQuery svq)
            throws PlanningException {

        List<ConnectorMetadata> availableConnectors = new ArrayList<>();
        availableConnectors.addAll(candidateConnectors);
        LogicalStep last = null;
        LogicalStep current = initial;
        List<ConnectorMetadata> toRemove = new ArrayList<>();
        boolean unregisteredFunction=false;
        String unregisteredFunctionMsg="";
        boolean exit = false;

        LOG.info("Available connectors: " + availableConnectors);

        while (!exit) {
            // Evaluate the connectors
            for (ConnectorMetadata connector : availableConnectors) {

                for (Operations currentOperation : current.getOperations()) {
                    if (!connector.supports(currentOperation)) {
                        // Check selector functions
                        toRemove.add(connector);
                        LOG.debug("Connector " + connector + " doesn't support : " + currentOperation);
                    } else {
                    /*
                     * This connector support the operation but we also have to check if support for a specific
                     * function is required support.
                     */
                        if (currentOperation.getOperationsStr().toLowerCase().contains("function")) {
                            Set<Project> previousInitialProjects = findPreviousInitialProjects(initial);
                            Set<String> sFunctions = MetadataManager.MANAGER.getSupportedFunctionNames(connector.getName(), previousInitialProjects);
                            switch (currentOperation) {
                            case SELECT_FUNCTIONS:
                                Select select = (Select) current;
                                Set<Selector> cols = select.getColumnMap().keySet();
                                if (!checkFunctionsConsistency(connector, sFunctions, cols, svq, previousInitialProjects)) {
                                    toRemove.add(connector);
                                    unregisteredFunction=true;
                                    unregisteredFunctionMsg= "Connector " + connector.getName() + " can't validate the " +
                                            "function: " + cols.toString();
                                    LOG.error(unregisteredFunctionMsg);
                                }
                                break;
                            case FILTER_FUNCTION_IN:
                            case FILTER_FUNCTION_BETWEEEN:
                            case FILTER_FUNCTION_DISTINCT:
                            case FILTER_FUNCTION_EQ:
                            case FILTER_FUNCTION_GET:
                            case FILTER_FUNCTION_GT:
                            case FILTER_FUNCTION_LET:
                            case FILTER_FUNCTION_LIKE:
                            case FILTER_FUNCTION_LT:
                            case FILTER_FUNCTION_NOT_BETWEEEN:
                            case FILTER_FUNCTION_NOT_IN:
                            case FILTER_FUNCTION_NOT_LIKE:
                                Filter filter = (Filter) current;
                                FunctionSelector functionSelector;
                                Set<Selector> cols2 = new HashSet<>();
                                if(FunctionSelector.class.isInstance(filter.getRelation().getLeftTerm())){
                                    functionSelector= ((FunctionSelector) filter.getRelation().getLeftTerm());
                                    cols2.add(functionSelector);
                                    cols2.add(filter.getRelation().getRightTerm());
                                }else{
                                    functionSelector= ((FunctionSelector) filter.getRelation().getRightTerm());
                                    cols2.add(functionSelector);
                                    cols2.add(filter.getRelation().getLeftTerm());
                                }

                                //Check function signature and its result
                                if (!checkFunctionsConsistency(connector, sFunctions, cols2, svq, previousInitialProjects)
                                        || (!checkFunctionsResultConsistency(connector, cols2))) {
                                    toRemove.add(connector);
                                    unregisteredFunction = true;
                                    unregisteredFunctionMsg= "Connector " + connector.getName() + " can't validate the " +
                                            "function: " + cols2.toString();
                                    LOG.error(unregisteredFunctionMsg);
                                }
                                break;
                                case FILTER_FUNCTION:
                                    break;
                            default:
                                throw new PlanningException(currentOperation + " not supported yet.");
                            }
                        }
                    }
                    if (current instanceof Virtualizable && ((Virtualizable) current).isVirtual()
                            && !connector.supports(Operations.SELECT_SUBQUERY)) {
                        toRemove.add(connector);
                    }
                }
            }

            // Remove invalid connectors
            if (toRemove.size() == availableConnectors.size()) {
                if (unregisteredFunction){
                    throw new PlanningException(unregisteredFunctionMsg);
                } else {
                    throw new PlanningException(
                            "Cannot determine execution path as no connector supports " + current.toString());
                }
            } else {
                availableConnectors.removeAll(toRemove);

                if (current.getNextStep() == null || UnionStep.class.isInstance(current.getNextStep())) {
                    exit = true;
                    last = current;
                } else {
                    current = current.getNextStep();
                }
            }
            toRemove.clear();
        }
        return new ExecutionPath(initial, last, availableConnectors);
    }

    /**
     * Checks whether the selectors are consistent with the functions supported by the connector.
     *
     * @param connectorMetadata  The connector metadata.
     * @param supportedFunctions The functions which are supported by the connector.
     * @param selectors          The set of selector
     * @return true if the selectors are consistent with the functions;
     * @throws PlanningException
     */
    private boolean checkFunctionsConsistency(ConnectorMetadata connectorMetadata, Set<String> supportedFunctions,
            Set<Selector> selectors, SelectValidatedQuery svq, Set<Project> initialProjects) throws PlanningException {

        boolean areFunctionsConsistent = true;
        Iterator<Selector> selectorIterator = selectors.iterator();
        FunctionSelector fSelector=null;
        while (selectorIterator.hasNext() && areFunctionsConsistent) {
            Selector selector = selectorIterator.next();
            if (selector instanceof FunctionSelector) {
                fSelector = (FunctionSelector) selector;
                if (!supportedFunctions.contains(fSelector.getFunctionName().toUpperCase())) {
                    areFunctionsConsistent = false;
                    break;
                } else {
                    if (!MetadataManager.MANAGER.checkInputSignature(fSelector, connectorMetadata.getName(), svq.getSubqueryValidatedQuery(), initialProjects)) {

                        areFunctionsConsistent = false;
                        break;
                    }
                }
                areFunctionsConsistent = checkFunctionsConsistency(
                        connectorMetadata, supportedFunctions,
                        new HashSet<>(fSelector.getFunctionColumns()), svq, initialProjects);
            }
        }
        return areFunctionsConsistent;
    }

    
    private boolean checkFunctionsResultConsistency(ConnectorMetadata connectorMetadata, Set<Selector> selectors) throws PlanningException {

        boolean areFunctionsConsistent = true;
        FunctionSelector fSelector=null;
        Selector retSelector=null;
        for (Selector selector:selectors){
            if (FunctionSelector.class.isInstance(selector)){
                fSelector=(FunctionSelector)selector;
            }else{
                retSelector=selector;
            }
        }
        //check return signature
        if (!MetadataManager.MANAGER.checkFunctionReturnSignature(fSelector, retSelector,
                connectorMetadata.getName() )) {
            areFunctionsConsistent = false;

        }
        return areFunctionsConsistent;
    }


    /**
     * Get the list of tables accessed in a list of initial steps.
     *
     * @param initialSteps The list of initial steps.
     * @return A list of {@link com.stratio.crossdata.common.data.TableName}.
     */
    protected List<TableName> getInitialSteps(List<LogicalStep> initialSteps) {
        List<TableName> tables = new ArrayList<>(initialSteps.size());
        for (LogicalStep ls: initialSteps) {
            tables.add(Project.class.cast(ls).getTableName());
        }
        return tables;
    }

    /**
     * Build a workflow with the {@link com.stratio.crossdata.common.logicalplan.LogicalStep} required to
     * execute a query. This method does not determine which connector will execute which part of the
     * workflow.
     *
     * @param query The query to be planned.
     * @return A Logical workflow.
     */
    protected LogicalWorkflow buildWorkflow(SelectValidatedQuery query) throws PlanningException {

        List<LogicalStep> initialSteps = new ArrayList<>();

        Map<String, TableMetadata> tableMetadataMap = new LinkedHashMap<>();
        for (TableMetadata tm: query.getTableMetadata()) {
            tableMetadataMap.put(tm.getName().getQualifiedName(), tm);
        }
        SelectStatement ss = SelectStatement.class.cast(query.getStatement());

        //Define the list of projects
        Map<String, LogicalStep> processed = getLogicalStepMap(query, tableMetadataMap, ss);

        //Initial steps.
        LogicalStep initial = null;
        for (LogicalStep ls: processed.values()) {
            if (!UnionStep.class.isInstance(ls)) {
                initial = ls;
                //Go to the first element of the workflow
                while (initial.getFirstPrevious() != null) {
                    initial = initial.getFirstPrevious();
                }
                if (Project.class.isInstance(initial)) {
                    initialSteps.add(initial);
                }
            }
        }

        //Find the last element
        LogicalStep last = initial;
        while (last.getNextStep() != null) {
            last = last.getNextStep();
        }

        // GROUP BY clause
        if (ss.isGroupInc()) {
            GroupBy groupBy;
            if(ss.isHavingInc()){
                groupBy = new GroupBy(
                        Collections.singleton(Operations.SELECT_GROUP_BY),
                        ss.getGroupByClause().getSelectorIdentifier(),
                        ss.getHavingClause());
            } else {
                groupBy = new GroupBy(
                        Collections.singleton(Operations.SELECT_GROUP_BY),
                        ss.getGroupByClause().getSelectorIdentifier());
            }

            last.setNextStep(groupBy);
            groupBy.setPrevious(last);
            last = groupBy;
        }

        // ORDER BY clause
        if (ss.isOrderInc()) {
            OrderBy orderBy = new OrderBy(
                    Collections.singleton(Operations.SELECT_ORDER_BY),
                    ss.getOrderByClauses());
            last.setNextStep(orderBy);
            orderBy.setPrevious(last);
            last = orderBy;
        }

        //Add LIMIT clause
        if (ss.isLimitInc()) {
            Limit l = new Limit(
                    Collections.singleton(Operations.SELECT_LIMIT),
                    ss.getLimit());
            last.setNextStep(l);
            l.setPrevious(last);
            last = l;
        }

        //Add SELECT operator
        Select finalSelect = generateSelect(ss, tableMetadataMap);
        if (Select.class.isInstance(last)) {
            //redirect last position to final select
            last = initial;
            while (last.getNextStep().getNextStep() != null) {
                last = last.getNextStep();
            }

        }
        last.setNextStep(finalSelect);
        finalSelect.setPrevious(last);

        LogicalWorkflow workflow = new LogicalWorkflow(initialSteps);

        if (query.getSubqueryValidatedQuery() != null) {
            LogicalWorkflow subqueryWorkflow = buildWorkflow(query.getSubqueryValidatedQuery());
            workflow = rearrangeWorkflow(workflow, subqueryWorkflow);
        }

        workflow.setLastStep(finalSelect);

        return workflow;
    }

    private Map<String, LogicalStep> getLogicalStepMap(SelectValidatedQuery query,
            Map<String, TableMetadata> tableMetadataMap, SelectStatement ss) throws PlanningException {

        Map<String, LogicalStep> processed = getProjects(query, tableMetadataMap);
        addProjectedColumns(processed, query);

        //Add filters
        if (query.getBasicRelations() != null) {
            processed = addFilter(processed, tableMetadataMap, query);
        }

        //Add window
        if (ss.getWindow() != null) {
            processed = addWindow(processed, ss);
        }

        //Add join
        if (!query.getJoinList().isEmpty()) {
            processed = addJoin((LinkedHashMap) processed, query);
        }

        //Add ComposeFilters
        if(query.getComposeRelations() != null){
            processed = addComposeFilters(processed, tableMetadataMap,  query);
        }
        return processed;
    }

    //TODO refactor with addFilters
    private Map<String,LogicalStep> addComposeFilters(Map<String, LogicalStep> lastSteps, Map<String, TableMetadata> tableMetadataMap, SelectValidatedQuery query) throws PlanningException {

        for(AbstractRelation ar: query.getComposeRelations()) {
            if (ar instanceof Relation) {
                Relation r = (Relation) ar;

                Selector s = r.getLeftTerm();
                Operations op = createOperation(tableMetadataMap, s, r);
                if (op != null) {
                    convertSelectSelectors(r);
                    Filter f = new Filter(
                            Collections.singleton(op),
                            r);
                    //Get sourceIdentifiers from ...
                    Set<TableName> abstractRelationTables = r.getAbstractRelationTables();
                    List<String> sourceIdentifiersFromAbstractRelationTable = new ArrayList<>(abstractRelationTables.size());
                    for (TableName abstractRelationTable : abstractRelationTables) {
                        sourceIdentifiersFromAbstractRelationTable.add(abstractRelationTable.getQualifiedName());
                    }
                    for (LogicalStep step : lastSteps.values()) {
                        if (UnionStep.class.isInstance(step)){
                            if( ((com.stratio.crossdata.common.logicalplan.Join) step).getSourceIdentifiers().containsAll(sourceIdentifiersFromAbstractRelationTable) ){
                                LogicalStep stepNextStep = step.getNextStep();
                                if (stepNextStep != null){
                                    f.setNextStep(stepNextStep);
                                    if (TransformationStep.class.isInstance(stepNextStep)){
                                        TransformationStep.class.cast(stepNextStep).setPrevious(f);
                                    }else if (UnionStep.class.isInstance(stepNextStep)){
                                        UnionStep.class.cast(stepNextStep).removePreviousStep(step);
                                        UnionStep.class.cast(stepNextStep).addPreviousSteps(step);
                                    }
                                }
                                step.setNextStep(f);
                                f.setPrevious(step);
                                break; //TODO remove
                            }
                        }
                    }

                } else {
                    LOG.error("Cannot determine Filter for relation " + r.toString() +
                            " on table " + s.getSelectorTablesAsString());
                }
            } else if (ar instanceof RelationDisjunction) {
                RelationDisjunction rd = (RelationDisjunction) ar;
                Operations op = Operations.FILTER_DISJUNCTION;
                List<List<ITerm>> filters = new ArrayList<>();
                List<RelationTerm> terms = rd.getTerms();
                for (RelationTerm rt : terms) {
                    List<ITerm> termList = new ArrayList<>();
                    for (AbstractRelation ab : rt.getRelations()) {
                        termList.addAll(createFilter(tableMetadataMap, ab));
                    }
                    filters.add(termList);
                }
                Disjunction d = new Disjunction(Collections.singleton(op), filters);

                //Get sourceIdentifiers from ...
                Set<TableName> abstractRelationTables = ar.getAbstractRelationTables();
                List<String> sourceIdentifiersFromAbstractRelationTable = new ArrayList<>(abstractRelationTables.size());
                for (TableName abstractRelationTable : abstractRelationTables) {
                    sourceIdentifiersFromAbstractRelationTable.add(abstractRelationTable.getQualifiedName());
                }
                for (LogicalStep step : lastSteps.values()) {
                    if (UnionStep.class.isInstance(step)){
                        if( ((com.stratio.crossdata.common.logicalplan.Join) step).getSourceIdentifiers().containsAll(sourceIdentifiersFromAbstractRelationTable) ){
                            LogicalStep stepNextStep = step.getNextStep();
                            if (stepNextStep != null){
                                d.setNextStep(stepNextStep);
                                if (TransformationStep.class.isInstance(stepNextStep)){
                                    TransformationStep.class.cast(stepNextStep).setPrevious(d);
                                }else if (UnionStep.class.isInstance(stepNextStep)){
                                    UnionStep.class.cast(stepNextStep).removePreviousStep(step);
                                    UnionStep.class.cast(stepNextStep).addPreviousSteps(step);
                                }
                            }
                            step.setNextStep(d);
                            d.setPrevious(step);
                        }
                    }
                }
                //TODO add last steps??
            }
        }
        return lastSteps;
    }

    private LogicalWorkflow rearrangeWorkflow(LogicalWorkflow workflow, LogicalWorkflow subqueryWorkflow) {
        if (workflow.getInitialSteps().size() == 1) {
            ((Project) workflow.getInitialSteps().get(0)).setPrevious(subqueryWorkflow.getLastStep());
            subqueryWorkflow.getLastStep().setNextStep(workflow.getInitialSteps().get(0));
        } else {

            Collection<String> outputAliasSelectors = ((Select) subqueryWorkflow.getLastStep()).getColumnMap().values();
            Collection<String> inputAliasSelectors;
            Iterator<LogicalStep> workflowIterator = workflow.getInitialSteps().iterator();
            while (workflowIterator.hasNext()) {
                inputAliasSelectors = new HashSet<>();
                Project wProject = (Project) workflowIterator.next();
                for (ColumnName columnName: wProject.getColumnList()) {
                    inputAliasSelectors.add(columnName.getName());
                }
                if (outputAliasSelectors.containsAll(inputAliasSelectors)) {
                    wProject.setPrevious(subqueryWorkflow.getLastStep());
                    subqueryWorkflow.getLastStep().setNextStep(wProject);
                } else {
                    subqueryWorkflow.getInitialSteps().add(wProject);
                }
            }
        }
        return subqueryWorkflow;
    }

    protected ExecutionWorkflow buildExecutionWorkflow(MetadataValidatedQuery query) throws PlanningException {
        MetadataStatement metadataStatement = query.getStatement();
        ExecutionWorkflow executionWorkflow;

        Set<String> metadataStatements = new HashSet<>();
        metadataStatements.add(CreateCatalogStatement.class.toString());
        metadataStatements.add(CreateTableStatement.class.toString());
        metadataStatements.add(DropCatalogStatement.class.toString());
        metadataStatements.add(DropTableStatement.class.toString());
        metadataStatements.add(AlterCatalogStatement.class.toString());
        metadataStatements.add(AlterTableStatement.class.toString());
        metadataStatements.add(CreateIndexStatement.class.toString());
        metadataStatements.add(DropIndexStatement.class.toString());
        metadataStatements.add(ImportMetadataStatement.class.toString());

        Set<String> managementStatements = new HashSet<>();
        managementStatements.add(AttachClusterStatement.class.toString());
        managementStatements.add(AttachConnectorStatement.class.toString());
        managementStatements.add(DetachConnectorStatement.class.toString());
        managementStatements.add(DetachClusterStatement.class.toString());
        managementStatements.add(AlterClusterStatement.class.toString());

        if (metadataStatements.contains(metadataStatement.getClass().toString())) {
            executionWorkflow = buildMetadataWorkflow(query);
        } else if (managementStatements.contains(metadataStatement.getClass().toString())) {
            executionWorkflow = buildManagementWorkflow(query);
        } else {
            throw new PlanningException("This statement can't be planned: " + metadataStatement.toString());
        }

        return executionWorkflow;
    }

    private MetadataWorkflow buildMetadataWorkflowCreateCatalog(MetadataStatement metadataStatement, String queryId) {
        MetadataWorkflow metadataWorkflow;
        // Create parameters for metadata workflow
        CreateCatalogStatement createCatalogStatement = (CreateCatalogStatement) metadataStatement;
        String actorRefUri = null;
        ExecutionType executionType = ExecutionType.CREATE_CATALOG;
        ResultType type = ResultType.RESULTS;

        metadataWorkflow = new MetadataWorkflow(queryId, actorRefUri, executionType, type);

        // Create & add CatalogMetadata to the MetadataWorkflow
        CatalogName name = createCatalogStatement.getCatalogName();
        Map<Selector, Selector> options = createCatalogStatement.getOptions();
        Map<TableName, TableMetadata> tables = new HashMap<>();
        CatalogMetadata catalogMetadata = new CatalogMetadata(name, options, tables);
        metadataWorkflow.setCatalogName(name);
        metadataWorkflow.setCatalogMetadata(catalogMetadata);
        metadataWorkflow.setIfNotExists(createCatalogStatement.isIfNotExists());
        return metadataWorkflow;
    }

    private MetadataWorkflow buildMetadataWorkflowCreateTable(MetadataStatement metadataStatement, String queryId) throws PlanningException {
        MetadataWorkflow metadataWorkflow;
        // Create parameters for metadata workflow
        CreateTableStatement createTableStatement = (CreateTableStatement) metadataStatement;
        String actorRefUri = null;
        ResultType type = ResultType.RESULTS;
        ExecutionType executionType;
        ClusterMetadata clusterMetadata = MetadataManager.MANAGER.getCluster(createTableStatement.getClusterName());

        // Create & add TableMetadata to the MetadataWorkflow
        ClusterName clusterName = createTableStatement.getClusterName();
        TableName tableName = createTableStatement.getTableName();
        TableMetadata tableMetadata = buildTableMetadata(createTableStatement);


        if (existsCatalogInCluster(tableName.getCatalogName(), clusterName)) {
            if (!createTableStatement.isExternal()) {

                Set<ConnectorName> connectorNames = clusterMetadata.getConnectorAttachedRefs().keySet();
                if (connectorNames.isEmpty()) {
                    throw new PlanningException("There is no connector attached to cluster " + clusterMetadata.getName().getName());
                }
                try {
                    actorRefUri = findAnyActorRef(clusterMetadata, Status.ONLINE, Operations.CREATE_TABLE);
                    executionType = ExecutionType.CREATE_TABLE;
                } catch (PlanningException pe) {
                    LOG.debug("No connector was found to execute CREATE_TABLE: " + System.lineSeparator() + pe.getMessage());
                    executionType = ExecutionType.REGISTER_TABLE;
                    for (ConnectorName connectorName : connectorNames) {
                        if (MetadataManager.MANAGER.getConnector(connectorName).getSupportedOperations().contains(Operations.CREATE_TABLE)) {
                            throw new PlanningException(connectorName.getQualifiedName() + " supports CREATE_TABLE but no connector was found to execute CREATE_TABLE");
                        }
                    }
                }
            } else {
                executionType = ExecutionType.REGISTER_TABLE;
            }
            metadataWorkflow = new MetadataWorkflow(queryId, actorRefUri, executionType, type);
            //The catalog needs to be associated with the cluster
        } else {

            if (!createTableStatement.isExternal()) {

                Set<ConnectorName> connectorNames = clusterMetadata.getConnectorAttachedRefs().keySet();
                if (connectorNames.isEmpty()) {
                    throw new PlanningException("There is no connector attached to cluster " + clusterMetadata.getName().getName());
                }
                try {
                    actorRefUri = findAnyActorRef(clusterMetadata, Status.ONLINE, Operations.CREATE_CATALOG, Operations.CREATE_TABLE);
                    executionType = ExecutionType.CREATE_TABLE_AND_CATALOG;
                } catch (PlanningException pe) {
                    LOG.debug("No connector was found to execute CREATE_CATALOG: " + System.lineSeparator() + pe.getMessage());
                    for (ConnectorName connectorName : connectorNames) {
                        if (MetadataManager.MANAGER.getConnector(connectorName).getSupportedOperations().contains(Operations.CREATE_CATALOG)) {
                            throw new PlanningException(connectorName.getQualifiedName() + " supports CREATE_CATALOG but no connector was found to execute CREATE_CATALOG AND CREATE_TABLE. The table should be registered");
                        }
                    }
                    //if there is no connector supporting create table an exception is thrown
                    actorRefUri = findAnyActorRef(clusterMetadata, Status.ONLINE, Operations.CREATE_TABLE);
                    executionType = ExecutionType.CREATE_TABLE_REGISTER_CATALOG;
                }

            //REGISTER TABLE
            } else {
                executionType = ExecutionType.REGISTER_TABLE_AND_CATALOG;
            }

            metadataWorkflow = new MetadataWorkflow(queryId, actorRefUri, executionType, type);
            metadataWorkflow.setCatalogName(createTableStatement.getTableName().getCatalogName());
            metadataWorkflow.setCatalogMetadata(MetadataManager.MANAGER
                    .getCatalog(tableName.getCatalogName()));

        }


        metadataWorkflow.setIfNotExists(createTableStatement.isIfNotExists());
        metadataWorkflow.setTableName(tableName);
        metadataWorkflow.setTableMetadata(tableMetadata);
        metadataWorkflow.setClusterName(clusterName);


        return metadataWorkflow;
    }





    private TableMetadata buildTableMetadata(CreateTableStatement createTableStatement) {
        TableName name = createTableStatement.getTableName();

        Map<Selector, Selector> options = createTableStatement.getProperties();
        LinkedHashMap<ColumnName, ColumnMetadata> columnMap = new LinkedHashMap<>();
        for (Map.Entry<ColumnName, ColumnType> c : createTableStatement.getColumnsWithTypes().entrySet()) {
            ColumnName columnName = c.getKey();
            ColumnMetadata columnMetadata = new ColumnMetadata(columnName, null, c.getValue());
            columnMap.put(columnName, columnMetadata);
        }
        ClusterName clusterName = createTableStatement.getClusterName();
        List<ColumnName> partitionKey = new LinkedList<>();
        partitionKey.addAll(createTableStatement.getPartitionKey());
        List<ColumnName> clusterKey = new LinkedList<>();
        clusterKey.addAll(createTableStatement.getClusterKey());
        Map<IndexName, IndexMetadata> indexes = new HashMap<>();
        return new TableMetadata(name, options, columnMap, indexes, clusterName, partitionKey,
                clusterKey);

    }


    private MetadataWorkflow buildMetadataWorkflowDropCatalog(MetadataStatement metadataStatement, String queryId)
            throws PlanningException {
        MetadataWorkflow metadataWorkflow;
        DropCatalogStatement dropCatalogStatement = (DropCatalogStatement) metadataStatement;

        CatalogName catalog = dropCatalogStatement.getCatalogName();
        CatalogMetadata catalogMetadata = null;
        if (MetadataManager.MANAGER.exists(catalog)) {
            catalogMetadata = MetadataManager.MANAGER.getCatalog(catalog);
        }

        if (catalogMetadata == null ||
                catalogMetadata.getTables().isEmpty() ||
                catalogMetadata.getTables() == null) {
            MetadataManager.MANAGER.deleteCatalog(catalog, dropCatalogStatement.isIfExists());
            // Create MetadataWorkFlow
            metadataWorkflow = new MetadataWorkflow(queryId, null, ExecutionType.DROP_CATALOG, ResultType.RESULTS);
        } else {
            throw new PlanningException("This statement can't be planned: " + metadataStatement.toString() + ". " +
                    "All tables of the catalog must be removed before dropping the catalog.");
        }
        return metadataWorkflow;
    }

    private MetadataWorkflow buildMetadataWorkflowAlterCatalog(MetadataStatement metadataStatement, String queryId) {
        MetadataWorkflow metadataWorkflow;
        AlterCatalogStatement alterCatalogStatement = (AlterCatalogStatement) metadataStatement;
        CatalogName catalog = alterCatalogStatement.getCatalogName();
        CatalogMetadata catalogMetadata = MetadataManager.MANAGER.getCatalog(catalog);
        catalogMetadata.setOptions(alterCatalogStatement.getOptions());

        metadataWorkflow = new MetadataWorkflow(queryId, null, ExecutionType.ALTER_CATALOG, ResultType.RESULTS);

        metadataWorkflow.setCatalogMetadata(catalogMetadata);
        metadataWorkflow.setCatalogName(catalogMetadata.getName());
        return metadataWorkflow;
    }

    private MetadataWorkflow buildMetadataWorkflowCreateIndex(MetadataStatement metadataStatement, String queryId)
            throws PlanningException {
        MetadataWorkflow metadataWorkflow;
        CreateIndexStatement createIndexStatement = (CreateIndexStatement) metadataStatement;

        TableMetadata tableMetadata = MetadataManager.MANAGER.getTable(createIndexStatement.getTableName());

        ClusterName clusterName = tableMetadata.getClusterRef();
        ClusterMetadata clusterMetadata = MetadataManager.MANAGER.getCluster(clusterName);

        String actorRefUri = findAnyActorRef(clusterMetadata, Status.ONLINE, Operations.CREATE_INDEX);

        metadataWorkflow = new MetadataWorkflow(queryId, actorRefUri, ExecutionType.CREATE_INDEX, ResultType.RESULTS);

        metadataWorkflow.setIndexName(createIndexStatement.getName());
        metadataWorkflow.setIfNotExists(createIndexStatement.isCreateIfNotExists());
        metadataWorkflow.setClusterName(clusterMetadata.getName());
        IndexName name = createIndexStatement.getName();

        Map<ColumnName, ColumnMetadata> columns = new HashMap<>();
        Set<ColumnName> targetColumns = createIndexStatement.getTargetColumns();
        for (ColumnName columnName: targetColumns) {
            ColumnMetadata columnMetadata = MetadataManager.MANAGER.getColumn(columnName);
            columns.put(columnName, columnMetadata);
        }
        IndexType type = createIndexStatement.getType();
        Map<Selector, Selector> options = createIndexStatement.getOptions();
        IndexMetadata indexMetadata = new IndexMetadata(name, columns, type, options);
        metadataWorkflow.setIndexMetadata(indexMetadata);
        return metadataWorkflow;
    }

    private MetadataWorkflow buildMetadataWorkflowDropIndex(MetadataStatement metadataStatement, String queryId)
            throws PlanningException {

        MetadataWorkflow metadataWorkflow;
        DropIndexStatement dropIndexStatement = (DropIndexStatement) metadataStatement;

        IndexName indexName = dropIndexStatement.getName();

        TableMetadata tableMetadata = MetadataManager.MANAGER.getTable(indexName.getTableName());

        ClusterName clusterName = tableMetadata.getClusterRef();
        ClusterMetadata clusterMetadata = MetadataManager.MANAGER.getCluster(clusterName);

        String actorRefUri = findAnyActorRef(clusterMetadata, Status.ONLINE, Operations.DROP_INDEX);

        metadataWorkflow = new MetadataWorkflow(queryId, actorRefUri, ExecutionType.DROP_INDEX, ResultType.RESULTS);

        metadataWorkflow.setIndexName(dropIndexStatement.getName());
        metadataWorkflow.setIfExists(dropIndexStatement.isDropIfExists());
        metadataWorkflow.setClusterName(clusterMetadata.getName());

        metadataWorkflow.setIndexMetadata(tableMetadata.getIndexes().get(indexName));
        return metadataWorkflow;
    }

    private MetadataWorkflow buildMetadataWorkflowDropTable(MetadataStatement metadataStatement, String queryId)
            throws PlanningException {

        MetadataWorkflow metadataWorkflow;
        DropTableStatement dropTableStatement = (DropTableStatement) metadataStatement;
        TableMetadata tableMetadata = MetadataManager.MANAGER.getTable(dropTableStatement.getTableName());
        ClusterMetadata clusterMetadata= MetadataManager.MANAGER.getCluster(tableMetadata.getClusterRef());
        String actorRefUri = null;

        ExecutionType executionType = ExecutionType.DROP_TABLE;
        ResultType type = ResultType.RESULTS;

        if (!dropTableStatement.isExternal()) {
            actorRefUri = findAnyActorRef(clusterMetadata, Status.ONLINE, Operations.DROP_TABLE);
        }else{
            executionType = ExecutionType.UNREGISTER_TABLE;
        }
        metadataWorkflow = new MetadataWorkflow(queryId, actorRefUri, executionType, type);

        metadataWorkflow.setTableMetadata(tableMetadata);
        metadataWorkflow.setTableName(tableMetadata.getName());
        metadataWorkflow.setClusterName(clusterMetadata.getName());

        return metadataWorkflow;
    }

    private MetadataWorkflow buildMetadataWorkflowAlterTable(MetadataStatement metadataStatement, String queryId)
            throws PlanningException {

        MetadataWorkflow metadataWorkflow;
        AlterTableStatement alterTableStatement = (AlterTableStatement) metadataStatement;

        ExecutionType executionType = ExecutionType.ALTER_TABLE;
        ResultType type = ResultType.RESULTS;

        TableMetadata tableMetadata = MetadataManager.MANAGER.getTable(alterTableStatement.getTableName());

        ColumnMetadata alterColumnMetadata = alterTableStatement.getColumnMetadata();

        AlterOptions alterOptions;
        switch (alterTableStatement.getOption()) {
        case ADD_COLUMN:
            alterOptions = new AlterOptions(AlterOperation.ADD_COLUMN, null, alterColumnMetadata);
            break;
        case ALTER_COLUMN:
            alterOptions = new AlterOptions(AlterOperation.ALTER_COLUMN, null, alterColumnMetadata);
            break;
        case DROP_COLUMN:
            alterOptions = new AlterOptions(AlterOperation.DROP_COLUMN, null, alterColumnMetadata);
            break;
        case ALTER_OPTIONS:
            alterOptions = new AlterOptions(AlterOperation.ALTER_OPTIONS, alterTableStatement.getProperties(),
                    alterColumnMetadata);
            break;
        default:
            throw new PlanningException("This statement can't be planned: " + metadataStatement.toString());
        }

        ClusterName clusterName = tableMetadata.getClusterRef();
        ClusterMetadata clusterMetadata = MetadataManager.MANAGER.getCluster(clusterName);

        String actorRefUri = findAnyActorRef(clusterMetadata, Status.ONLINE, Operations.ALTER_TABLE);

        metadataWorkflow = new MetadataWorkflow(queryId, actorRefUri, executionType, type);

        metadataWorkflow.setTableName(tableMetadata.getName());
        metadataWorkflow.setAlterOptions(alterOptions);
        metadataWorkflow.setClusterName(clusterMetadata.getName());

        return metadataWorkflow;
    }

    private MetadataWorkflow buildMetadataWorkflowImportMetadata(MetadataStatement metadataStatement, String queryId)
            throws PlanningException {

        MetadataWorkflow metadataWorkflow;
        ImportMetadataStatement importMetadataStatement = (ImportMetadataStatement) metadataStatement;

        ExecutionType executionType = ExecutionType.DISCOVER_METADATA;
        if (!importMetadataStatement.isDiscover()) {
            executionType = ExecutionType.IMPORT_CATALOGS;
            if (importMetadataStatement.getTableName() != null) {
                executionType = ExecutionType.IMPORT_TABLE;
            } else if (importMetadataStatement.getCatalogName() != null) {
                executionType = ExecutionType.IMPORT_CATALOG;
            }
        }

        ResultType type = ResultType.RESULTS;

        ClusterName clusterName = importMetadataStatement.getClusterName();
        ClusterMetadata clusterMetadata = MetadataManager.MANAGER.getCluster(clusterName);

        String actorRefUri = findAnyActorRef(clusterMetadata, Status.ONLINE, Operations.IMPORT_METADATA);

        metadataWorkflow = new MetadataWorkflow(queryId, actorRefUri, executionType, type);

        metadataWorkflow.setClusterName(clusterName);
        metadataWorkflow.setCatalogName(importMetadataStatement.getCatalogName());
        metadataWorkflow.setTableName(importMetadataStatement.getTableName());

        return metadataWorkflow;
    }

    private ExecutionWorkflow buildMetadataWorkflow(MetadataValidatedQuery query) throws PlanningException {
        MetadataStatement metadataStatement = query.getStatement();
        String queryId = query.getQueryId();
        MetadataWorkflow metadataWorkflow;

        if (metadataStatement instanceof CreateCatalogStatement) {
            metadataWorkflow = buildMetadataWorkflowCreateCatalog(metadataStatement, queryId);
        } else if (metadataStatement instanceof CreateTableStatement) {
            metadataWorkflow = buildMetadataWorkflowCreateTable(metadataStatement, queryId);
        } else if (metadataStatement instanceof DropCatalogStatement) {
            metadataWorkflow = buildMetadataWorkflowDropCatalog(metadataStatement, queryId);
        } else if (metadataStatement instanceof AlterCatalogStatement) {
            metadataWorkflow = buildMetadataWorkflowAlterCatalog(metadataStatement, queryId);
        } else if (metadataStatement instanceof CreateIndexStatement) {
            metadataWorkflow = buildMetadataWorkflowCreateIndex(metadataStatement, queryId);
        } else if (metadataStatement instanceof DropIndexStatement) {
            metadataWorkflow = buildMetadataWorkflowDropIndex(metadataStatement, queryId);
        } else if (metadataStatement instanceof DropTableStatement) {
            metadataWorkflow = buildMetadataWorkflowDropTable(metadataStatement, queryId);
        } else if (metadataStatement instanceof AlterTableStatement) {
            metadataWorkflow = buildMetadataWorkflowAlterTable(metadataStatement, queryId);
            metadataWorkflow.setTableMetadata(MetadataManager.MANAGER.getTable(metadataWorkflow.getTableName()));
        } else if (metadataStatement instanceof ImportMetadataStatement) {
            metadataWorkflow = buildMetadataWorkflowImportMetadata(metadataStatement, queryId);
        } else {
            throw new PlanningException("This statement can't be planned: " + metadataStatement.toString());
        }





        return metadataWorkflow;
    }

    private ExecutionWorkflow buildManagementWorkflow(MetadataValidatedQuery query) throws PlanningException {
        MetadataStatement metadataStatement = query.getStatement();
        String queryId = query.getQueryId();
        ManagementWorkflow managementWorkflow;

        if (metadataStatement instanceof AttachClusterStatement) {

            // Create parameters for metadata workflow
            AttachClusterStatement attachClusterStatement = (AttachClusterStatement) metadataStatement;
            ExecutionType executionType = ExecutionType.ATTACH_CLUSTER;
            ResultType type = ResultType.RESULTS;

            managementWorkflow = new ManagementWorkflow(queryId, "", executionType, type);

            // Add required information
            managementWorkflow.setClusterName(attachClusterStatement.getClusterName());
            managementWorkflow.setDatastoreName(attachClusterStatement.getDatastoreName());
            managementWorkflow.setOptions(attachClusterStatement.getOptions());

        } else if (metadataStatement instanceof DetachClusterStatement) {
            DetachClusterStatement detachClusterStatement = (DetachClusterStatement) metadataStatement;
            ExecutionType executionType = ExecutionType.DETACH_CLUSTER;
            ResultType type = ResultType.RESULTS;

            managementWorkflow = new ManagementWorkflow(queryId, "", executionType, type);
            String clusterName = detachClusterStatement.getClusterName();
            managementWorkflow.setClusterName(new ClusterName(clusterName));

        } else if (metadataStatement instanceof AttachConnectorStatement) {
            // Create parameters for metadata workflow
            AttachConnectorStatement attachConnectorStatement = (AttachConnectorStatement) metadataStatement;
            ExecutionType executionType = ExecutionType.ATTACH_CONNECTOR;
            ResultType type = ResultType.RESULTS;

            ConnectorMetadata connector = MetadataManager.MANAGER
                    .getConnector(attachConnectorStatement.getConnectorName());

            managementWorkflow = new ManagementWorkflow(queryId, connector.getActorRefs(), executionType, type);

            // Add required information
            managementWorkflow.setConnectorName(attachConnectorStatement.getConnectorName());
            managementWorkflow.setClusterName(attachConnectorStatement.getClusterName());
            managementWorkflow.setOptions(attachConnectorStatement.getOptions());
            managementWorkflow.setPageSize(attachConnectorStatement.getPagination());
            managementWorkflow.setPriority(attachConnectorStatement.getPriority());

        } else if (metadataStatement instanceof DetachConnectorStatement) {
            DetachConnectorStatement detachConnectorStatement = (DetachConnectorStatement) metadataStatement;
            ExecutionType executionType = ExecutionType.DETACH_CONNECTOR;
            ResultType type = ResultType.RESULTS;

            ConnectorMetadata connector = MetadataManager.MANAGER
                    .getConnector(detachConnectorStatement.getConnectorName());

            managementWorkflow = new ManagementWorkflow(queryId, connector.getActorRefs(), executionType, type);
            managementWorkflow.setConnectorName(detachConnectorStatement.getConnectorName());
            managementWorkflow.setClusterName(detachConnectorStatement.getClusterName());

        } else if (metadataStatement instanceof AlterClusterStatement) {
            AlterClusterStatement alterClusterStatement = (AlterClusterStatement) metadataStatement;
            ExecutionType executionType = ExecutionType.ALTER_CLUSTER;
            ResultType type = ResultType.RESULTS;

            ClusterMetadata clusterMetadata = MetadataManager.MANAGER
                    .getCluster(alterClusterStatement.getClusterName());

            managementWorkflow = new ManagementWorkflow(queryId, "", executionType, type);
            managementWorkflow.setClusterName(alterClusterStatement.getClusterName());
            managementWorkflow.setOptions(alterClusterStatement.getOptions());
            managementWorkflow.setDatastoreName(clusterMetadata.getDataStoreRef());

        } else {
            throw new PlanningException("This statement can't be planned: " + metadataStatement.toString());
        }

        return managementWorkflow;
    }

    /**
     * Check if a catalog was already registered in a cluster.
     *
     * @param catalogName catalog to be searched.
     * @param clusterName cluster that should contain the catalog.
     * @return if the catalog was found in the cluster.
     */
    private boolean existsCatalogInCluster(CatalogName catalogName, ClusterName clusterName) {
        ClusterMetadata cluster = MetadataManager.MANAGER.getCluster(clusterName);
        boolean result = false;
        if (cluster.getPersistedCatalogs().contains(catalogName)) {
            return true;
        }
        return result;
    }

    private StorageWorkflow buildExecutionWorkflowInsert(StorageValidatedQuery query, String queryId)
            throws PlanningException {
        StorageWorkflow storageWorkflow = null;
        InsertIntoStatement insertIntoStatement = (InsertIntoStatement) query.getStatement();

        TableName tableName = insertIntoStatement.getTableName();
        TableMetadata tableMetadata = getTableMetadata(tableName);
        ClusterMetadata clusterMetadata = getClusterMetadata(tableMetadata.getClusterRef());

        String actorRef;

        if (insertIntoStatement.isIfNotExists()) {
            actorRef = findAnyActorRef(clusterMetadata, Status.ONLINE, Operations.INSERT_IF_NOT_EXISTS);
        } else {
            actorRef = findAnyActorRef(clusterMetadata, Status.ONLINE, Operations.INSERT);
        }

        if (insertIntoStatement.getTypeValues() == InsertIntoStatement.TYPE_VALUES_CLAUSE) {

            storageWorkflow = new StorageWorkflow(queryId, actorRef, ExecutionType.INSERT, ResultType.RESULTS);
            storageWorkflow.setClusterName(tableMetadata.getClusterRef());
            storageWorkflow.setTableMetadata(tableMetadata);
            storageWorkflow.setIfNotExists(insertIntoStatement.isIfNotExists());

            Row row = getInsertRow(insertIntoStatement);
            storageWorkflow.setRow(row);

        } else if (insertIntoStatement.getTypeValues() == InsertIntoStatement.TYPE_SELECT_CLAUSE) {

            // PLAN SELECT
            SelectStatement selectStatement = insertIntoStatement.getSelectStatement();

            BaseQuery selectBaseQuery = new BaseQuery(
                    query.getQueryId(),
                    selectStatement.toString(),
                    query.getDefaultCatalog(), UUID.randomUUID().toString());
            SelectParsedQuery selectParsedQuery = new SelectParsedQuery(selectBaseQuery, selectStatement);

            Validator validator = new Validator();
            SelectValidatedQuery selectValidatedQuery;
            try {
                selectValidatedQuery = (SelectValidatedQuery) validator.validate(selectParsedQuery);
            } catch (ValidationException | IgnoreQueryException e) {
                throw new PlanningException(e.getMessage());
            }

            selectValidatedQuery.optimizeQuery();
            LogicalWorkflow selectLogicalWorkflow = buildWorkflow(selectValidatedQuery);
            selectLogicalWorkflow = addAliasFromInsertToSelect(insertIntoStatement, selectLogicalWorkflow);
            ExecutionWorkflow selectExecutionWorkflow = buildExecutionWorkflow(
                    selectValidatedQuery,
                    selectLogicalWorkflow);


            // FIND CANDIDATES
            List<ClusterName> involvedClusters = getClusterNames(insertIntoStatement, clusterMetadata);

            selectExecutionWorkflow.setResultType(ResultType.TRIGGER_EXECUTION);
            Set<Operations> requiredOperations = new HashSet<>();
            if (insertIntoStatement.isIfNotExists()){
                requiredOperations.add(Operations.INSERT_FROM_SELECT);
                requiredOperations.add(Operations.INSERT_IF_NOT_EXISTS);
                selectExecutionWorkflow.setTriggerStep(
                        new PartialResults(
                                Collections.singleton(Operations.INSERT_IF_NOT_EXISTS)));

            }else{
                requiredOperations.add(Operations.INSERT);
                requiredOperations.add(Operations.INSERT_FROM_SELECT);
                selectExecutionWorkflow.setTriggerStep(
                        new PartialResults(
                                Collections.singleton(Operations.INSERT)));
            }


            List<ConnectorMetadata> candidates = findCandidates(
                    involvedClusters,
                    requiredOperations);

            storageWorkflow = getStorageWorkflow(queryId, insertIntoStatement, tableMetadata, actorRef,
                    selectExecutionWorkflow, involvedClusters,
                    candidates);
        }
        return storageWorkflow;
    }

    private StorageWorkflow getStorageWorkflow(String queryId, InsertIntoStatement insertIntoStatement,
            TableMetadata tableMetadata, String actorRef, ExecutionWorkflow selectExecutionWorkflow,
            List<ClusterName> involvedClusters, List<ConnectorMetadata> candidates) {

        StorageWorkflow storageWorkflow;
        if ((candidates != null) && (!candidates.isEmpty())) {
            // Build a unique workflow

            //TODO INSERT FROM SELECT
            Iterator<ConnectorMetadata> connectorMetadata = getConnectorsSortedByPriority(candidates, involvedClusters).iterator();

            storageWorkflow = buildInsertFromSelectTriggeredStorageWorkflow(queryId, insertIntoStatement, tableMetadata, StringUtils.getAkkaActorRefUri(connectorMetadata.next().getActorRef(host), false), selectExecutionWorkflow, ExecutionType.INSERT_FROM_SELECT);
            StorageWorkflow tempWorkflow = storageWorkflow;
            while(connectorMetadata.hasNext()){
                StorageWorkflow auxWorkflow = buildInsertFromSelectTriggeredStorageWorkflow(queryId, insertIntoStatement, tableMetadata, StringUtils.getAkkaActorRefUri(connectorMetadata.next().getActorRef(host), false), selectExecutionWorkflow, ExecutionType.INSERT_FROM_SELECT);
                tempWorkflow.setLowPriorityExecutionWorkflow(auxWorkflow);
                tempWorkflow = auxWorkflow;
            }

        } else {
            // Build a workflow for select and insert
            storageWorkflow = buildInsertFromSelectTriggeredStorageWorkflow(queryId, insertIntoStatement, tableMetadata, actorRef, selectExecutionWorkflow, ExecutionType.INSERT_BATCH);
        }
        return storageWorkflow;
    }

    private StorageWorkflow buildInsertFromSelectTriggeredStorageWorkflow(String queryId, InsertIntoStatement insertIntoStatement, TableMetadata tableMetadata, String actorRef, ExecutionWorkflow selectExecutionWorkflow, ExecutionType exType) {
        StorageWorkflow storageWorkflow = new StorageWorkflow(queryId, actorRef, exType, ResultType.RESULTS);
        storageWorkflow.setClusterName(tableMetadata.getClusterRef());
        storageWorkflow.setTableMetadata(tableMetadata);
        storageWorkflow.setIfNotExists(insertIntoStatement.isIfNotExists());
        storageWorkflow.setPreviousExecutionWorkflow(selectExecutionWorkflow);
        return storageWorkflow;
    }


    private List<ClusterName> getClusterNames(InsertIntoStatement insertIntoStatement, ClusterMetadata clusterMetadata)
            throws PlanningException {
        List<ClusterName> involvedClusters = new ArrayList<>();
        involvedClusters.add(clusterMetadata.getName());
        for (TableName tableNameFromSelect: insertIntoStatement.getSelectStatement().getFromTables()) {
            TableMetadata tableMetadataFromSelect = getTableMetadata(tableNameFromSelect);
            if (!involvedClusters.contains(tableMetadataFromSelect.getClusterRef())) {
                involvedClusters.add(tableMetadataFromSelect.getClusterRef());
            }
        }
        return involvedClusters;
    }

    private LogicalWorkflow addAliasFromInsertToSelect(
            InsertIntoStatement insertIntoStatement,
            LogicalWorkflow selectLogicalWorkflow) {
        Select lastStep = (Select) selectLogicalWorkflow.getLastStep();

        List<ColumnName> insertColumns = insertIntoStatement.getColumns();

        // COLUMN MAP
        Map<Selector, String> columnMap = lastStep.getColumnMap();
        Map<Selector, String> newColumnMap = new LinkedHashMap<>();
        int i = 0;
        for (Map.Entry<Selector, String> column: columnMap.entrySet()) {
            ColumnName columnName = insertColumns.get(i);
            Selector newSelector = column.getKey();
            newSelector.setAlias(columnName.getName());
            newColumnMap.put(newSelector, columnName.getName());
            i++;
        }
        lastStep.setColumnMap(newColumnMap);

        //
        Map<String, ColumnType> typeMap = lastStep.getTypeMap();
        Map<String, ColumnType> newTypeMap = new LinkedHashMap<>();
        i = 0;
        for (Map.Entry<String, ColumnType> column: typeMap.entrySet()) {
            ColumnName columnName = insertColumns.get(i);
            ColumnType columnType = column.getValue();
            newTypeMap.put(columnName.getName(), columnType);
            i++;
        }
        lastStep.setTypeMap(newTypeMap);

        //
        Map<Selector, ColumnType> typeMapFromColumnName = lastStep.getTypeMapFromColumnName();
        Map<Selector, ColumnType> newTypeMapFromColumnName = new LinkedHashMap<>();
        i = 0;
        for (Map.Entry<Selector, ColumnType> column: typeMapFromColumnName.entrySet()) {
            ColumnName columnName = insertColumns.get(i);
            Selector newSelector = column.getKey();
            newSelector.setAlias(columnName.getName());
            ColumnType columnType = column.getValue();
            newTypeMapFromColumnName.put(newSelector, columnType);
            i++;
        }
        lastStep.setTypeMapFromColumnName(newTypeMapFromColumnName);

        return selectLogicalWorkflow;
    }

    private List<ConnectorMetadata> findCandidates(List<ClusterName> involvedClusters,
            Set<Operations> requiredOperations) {
        List<ConnectorMetadata> candidates = new ArrayList<>();
        if ((involvedClusters != null) && (requiredOperations != null)) {
            List<ConnectorMetadata> allConnectors = MetadataManager.MANAGER.getConnectors();
            for (ConnectorMetadata connectorMetadata: allConnectors) {
                if (connectorMetadata.getClusterRefs().containsAll(involvedClusters)) {
                    if (connectorMetadata.getSupportedOperations().containsAll(requiredOperations)) {
                        candidates.add(connectorMetadata);
                    }
                }
            }
        }
        return candidates;
    }

    private StorageWorkflow buildExecutionWorkflowDelete(StorageValidatedQuery query, String queryId)
            throws PlanningException {
        StorageWorkflow storageWorkflow;
        DeleteStatement deleteStatement = (DeleteStatement) query.getStatement();

        // Find connector
        String actorRef;
        TableMetadata tableMetadata = getTableMetadata(deleteStatement.getTableName());
        ClusterMetadata clusterMetadata = getClusterMetadata(tableMetadata.getClusterRef());

        List<Filter> filters = new ArrayList<>();
        Set<Operations> requiredOperations = new HashSet<>();

        List<Relation> relations = deleteStatement.getWhereClauses();
        if ((relations == null) || (relations.isEmpty())) {
            requiredOperations.add(Operations.DELETE_NO_FILTERS);
        } else {
            for (Relation relation: deleteStatement.getWhereClauses()) {
                Operations operation = getFilterOperation(tableMetadata, "DELETE", relation.getLeftTerm(),
                        relation.getOperator());
                Filter filter = new Filter(
                        Collections.singleton(operation),
                        relation);
                filters.add(filter);
                requiredOperations.addAll(filter.getOperations());
            }
        }

        actorRef = findAnyActorRef(clusterMetadata, Status.ONLINE,
                requiredOperations.toArray(new Operations[requiredOperations.size()]));

        storageWorkflow = new StorageWorkflow(queryId, actorRef, ExecutionType.DELETE_ROWS, ResultType.RESULTS);

        storageWorkflow.setClusterName(clusterMetadata.getName());
        storageWorkflow.setTableName(tableMetadata.getName());

        storageWorkflow.setWhereClauses(filters);
        return storageWorkflow;
    }

    private StorageWorkflow buildExecutionWorkflowUpdate(StorageValidatedQuery query, String queryId)
            throws PlanningException {
        StorageWorkflow storageWorkflow;
        UpdateTableStatement updateTableStatement = (UpdateTableStatement) query.getStatement();

        // Find connector
        String actorRef;
        TableMetadata tableMetadata = getTableMetadata(updateTableStatement.getTableName());
        ClusterMetadata clusterMetadata = getClusterMetadata(tableMetadata.getClusterRef());

        List<Filter> filters = new ArrayList<>();
        Set<Operations> requiredOperations = new HashSet<>();

        List<Relation> relations = updateTableStatement.getWhereClauses();
        if ((relations == null) || (relations.isEmpty())) {
            requiredOperations.add(Operations.UPDATE_NO_FILTERS);
        } else {
            for (Relation relation: updateTableStatement.getWhereClauses()) {
                Operations operation = getFilterOperation(tableMetadata, "UPDATE", relation.getLeftTerm(),
                        relation.getOperator());
                Filter filter = new Filter(
                        Collections.singleton(operation),
                        relation);
                filters.add(filter);
                requiredOperations.addAll(filter.getOperations());
            }
        }

        actorRef = findAnyActorRef(clusterMetadata, Status.ONLINE,
                requiredOperations.toArray(new Operations[requiredOperations.size()]));

        storageWorkflow = new StorageWorkflow(queryId, actorRef, ExecutionType.UPDATE_TABLE, ResultType.RESULTS);

        storageWorkflow.setClusterName(clusterMetadata.getName());
        storageWorkflow.setTableName(tableMetadata.getName());
        storageWorkflow.setAssignments(updateTableStatement.getAssignations());

        storageWorkflow.setWhereClauses(filters);

        return storageWorkflow;
    }

    private StorageWorkflow buildExecutionWorkflowTruncate(StorageValidatedQuery query, String queryId)
            throws PlanningException {
        StorageWorkflow storageWorkflow;
        TruncateStatement truncateStatement = (TruncateStatement) query.getStatement();

        // Find connector
        String actorRef;
        TableMetadata tableMetadata = getTableMetadata(truncateStatement.getTableName());
        ClusterMetadata clusterMetadata = getClusterMetadata(tableMetadata.getClusterRef());

        actorRef = findAnyActorRef(clusterMetadata, Status.ONLINE, Operations.TRUNCATE_TABLE);

        storageWorkflow = new StorageWorkflow(queryId, actorRef, ExecutionType.TRUNCATE_TABLE, ResultType.RESULTS);

        storageWorkflow.setClusterName(clusterMetadata.getName());
        storageWorkflow.setTableName(tableMetadata.getName());
        return storageWorkflow;
    }

    /**
     * Build a workflow to notify the write buffer config.
     */
    private StorageWorkflow buildExecutionWorkflowInsertBatch(StorageValidatedQuery query, String queryId) throws PlanningException {
        StorageWorkflow storageWorkflow = null;
        InsertBatchStatement insertIntoStatement = (InsertBatchStatement) query.getStatement();

        TableName tableName = insertIntoStatement.getTableName();
        TableMetadata tableMetadata = getTableMetadata(tableName);
        ClusterMetadata clusterMetadata = getClusterMetadata(tableMetadata.getClusterRef());

        List<ConnectorMetadata> connectorsMetadata = findCandidates(Arrays.asList(clusterMetadata.getName()), Sets.newHashSet(Operations.INSERT));
        //TODO INSERT_BATCH
        if (connectorsMetadata.isEmpty()){
            throw new PlanningException("There is no connector online supporting "+Operations.INSERT);
        }else{
            String actorRef = connectorsMetadata.get(0).getActorRef(this.host);
            //TODO use getActorRefs to create partitions => add partitions to storageWorkflow
            storageWorkflow = new StorageWorkflow(queryId, actorRef, ExecutionType.INSERT_RAW_BATCH, ResultType.RESULTS);
            storageWorkflow.setClusterName(tableMetadata.getClusterRef());
            storageWorkflow.setTableMetadata(tableMetadata);
            storageWorkflow.setIfNotExists(insertIntoStatement.isIfNotExists());
            storageWorkflow.setBatchRows(insertIntoStatement.getStringColumns(), insertIntoStatement.getRawRows());
        }

        return storageWorkflow;
    }

    protected ExecutionWorkflow buildExecutionWorkflow(StorageValidatedQuery query) throws PlanningException {
        StorageWorkflow storageWorkflow;
        String queryId = query.getQueryId();

        if (query.getStatement() instanceof InsertIntoStatement) {
            storageWorkflow = buildExecutionWorkflowInsert(query, queryId);
        } else if (query.getStatement() instanceof DeleteStatement) {
            storageWorkflow = buildExecutionWorkflowDelete(query, queryId);
        } else if (query.getStatement() instanceof UpdateTableStatement) {
            storageWorkflow = buildExecutionWorkflowUpdate(query, queryId);
        } else if (query.getStatement() instanceof TruncateStatement) {
            storageWorkflow = buildExecutionWorkflowTruncate(query, queryId);
        } else if (query.getStatement() instanceof InsertBatchStatement) {
            storageWorkflow = buildExecutionWorkflowInsertBatch(query, queryId);
        } else {
            throw new PlanningException("This statement is not supported yet");
        }

        return storageWorkflow;
    }

    private Row getInsertRow(InsertIntoStatement statement) throws PlanningException {
        Row row = new Row();

        List<Selector> values = statement.getCellValues();
        List<ColumnName> ids = statement.getColumns();

        for (int i = 0; i < ids.size(); i++) {
            ColumnName columnName = ids.get(i);
            Selector value = values.get(i);
            CoreUtils coreUtils = CoreUtils.create();
            Object cellContent;
            if(FunctionSelector.class.isInstance(value)){
                cellContent = ((FunctionSelector)value).toStringWithoutAlias();
            } else {
                cellContent = coreUtils.convertSelectorToObject(value, columnName);
            }
            Cell cell = new Cell(cellContent);
            row.addCell(columnName.getName(), cell);
        }
        return row;
    }

    private ClusterMetadata getClusterMetadata(ClusterName clusterRef) throws PlanningException {
        ClusterMetadata clusterMetadata = MetadataManager.MANAGER.getCluster(clusterRef);
        if (clusterMetadata == null) {
            throw new PlanningException("There is not cluster metadata for Storage Operation");
        }
        return clusterMetadata;
    }

    private TableMetadata getTableMetadata(TableName tableName) throws PlanningException {
        TableMetadata tableMetadata = MetadataManager.MANAGER.getTable(tableName);
        if (tableMetadata == null) {
            throw new PlanningException("There is not specified Table for Storage Operation");
        }
        return tableMetadata;
    }

    /**
     * Add the columns that need to be retrieved to the initial steps map.
     *
     * @param projectSteps The map associating table names to Project steps.
     * @param query        The query to be planned.
     */
    private void addProjectedColumns(Map<String, LogicalStep> projectSteps, SelectValidatedQuery query)
            throws PlanningException {
        for (ColumnName cn: query.getColumns()) {
            if(!projectSteps.containsKey(cn.getTableName().getQualifiedName())){
                throw new PlanningException("Table " + cn.getTableName().getQualifiedName() +
                        " is unknown in query: " + query.toString());
            }
            Project.class.cast(projectSteps.get(cn.getTableName().getQualifiedName())).addColumn(cn);
        }
    }

    /**
     * Get the filter operation depending on the type of column and the selector of the where clauses.
     *
     * @param tableMetadata The table metadata.
     * @param statement     Statement type.
     * @param selector      The relationship selector.
     * @param operator      The relationship operator.
     * @return An {@link com.stratio.crossdata.common.metadata.Operations} object.
     */
    protected Operations getFilterOperation(final TableMetadata tableMetadata, final String statement,
            final Selector selector, final Operator operator) {
        StringBuilder sb = new StringBuilder(statement.toUpperCase());

        sb.append("_");
        ColumnSelector cs = ColumnSelector.class.cast(selector);
        if (tableMetadata.isPK(cs.getName())) {
            sb.append("PK_");
        } else if (tableMetadata.isIndexed(cs.getName())) {
            sb.append("INDEXED_");
        } else {
            sb.append("NON_INDEXED_");
        }
        sb.append(operator.name());
        return Operations.valueOf(sb.toString());

    }

    /**
     * Add Filter operations after the Project. The Filter operations to be applied are associated
     * with the where clause found.
     *
     * @param lastSteps        The map associating table names to Project steps.
     * @param tableMetadataMap A map with the table metadata indexed by table name.
     * @param query            The query to be planned.
     * @return The resulting map of logical steps.
     */
    private Map<String, LogicalStep> addFilter(Map<String, LogicalStep> lastSteps,
                    Map<String, TableMetadata> tableMetadataMap, SelectValidatedQuery query) throws PlanningException {
        for(AbstractRelation ar: query.getBasicRelations()){
            if(ar instanceof Relation){
                Relation r = (Relation) ar;
                Selector s = r.getLeftTerm();
                Operations op = createOperation(tableMetadataMap, s, r);
                if (op != null) {
                    convertSelectSelectors(r);
                    Filter f = new Filter(Collections.singleton(op), r);
                    LogicalStep previous = lastSteps.get(s.getSelectorTablesAsString());
                    previous.setNextStep(f);
                    f.setPrevious(previous);
                    lastSteps.put(s.getSelectorTablesAsString(), f);
                } else {
                    LOG.error("Cannot determine Filter for relation " + r.toString() +
                            " on table " + s.getSelectorTablesAsString());
                }
            } else if(ar instanceof RelationDisjunction) {
                RelationDisjunction rd = (RelationDisjunction) ar;
                Operations op = Operations.FILTER_DISJUNCTION;
                List<List<ITerm>> filters = new ArrayList<>();
                List<RelationTerm> terms = rd.getTerms();
                for(RelationTerm rt: terms){
                    List<ITerm> termList = new ArrayList<>();
                    for(AbstractRelation ab: rt.getRelations()){
                        termList.addAll(createFilter(tableMetadataMap, ab));
                    }
                    filters.add(termList);
                }
                Disjunction d = new Disjunction(Collections.singleton(op), filters);
                LogicalStep previous = lastSteps.get(rd.getFirstSelectorTablesAsString());
                previous.setNextStep(d);
                d.setPrevious(previous);
                lastSteps.put(rd.getFirstSelectorTablesAsString(), d);
            } else if (ar instanceof FunctionRelation){
                FunctionRelation relation = (FunctionRelation) ar;
                Operations op = Operations.FILTER_FUNCTION;
                if (op != null) {

                    FunctionFilter filter = new FunctionFilter(Collections.singleton(op), relation);
                    LogicalStep previous = lastSteps.get(relation.getTableName());
                    previous.setNextStep(filter);
                    filter.setPrevious(previous);
                    lastSteps.put(relation.getTableName(), filter);
                } else {
                    LOG.error("Cannot determine Filter for relation " + relation.toString() +
                            " on table " + relation.getTableName());
                }

            }
        }
        return lastSteps;
    }

    private Operations createOperation(Map<String, TableMetadata> tableMetadataMap, Selector s, Relation r)
            throws PlanningException {
        Operations op = null;
        //TODO Support left-side functions that contain columns of several tables.
        TableMetadata tm = tableMetadataMap.get(s.getSelectorTablesAsString());
        if(s instanceof FunctionSelector){
            op = Operations.valueOf("FILTER_FUNCTION_" + r.getOperator().name());
            return op;
        }
        if (tm != null) {
            op = getFilterOperation(tm, "FILTER", s, r.getOperator());
        } else if (s.getTableName().isVirtual()) {
            op = Operations.valueOf("FILTER_NON_INDEXED_" + r.getOperator().name());
        }
        if(op != null){
            convertSelectSelectors(r);
        }
        return op;
    }

    private List<ITerm> createFilter(
            Map<String, TableMetadata> tableMetadataMap,
            AbstractRelation abstractRelation) throws PlanningException {
        List<ITerm> filters = new ArrayList<>();
        if(abstractRelation instanceof Relation){
            Relation relation = (Relation) abstractRelation;
            Operations op = createOperation(tableMetadataMap, relation.getLeftTerm(), relation);
            filters.add(new Filter(
                    Collections.singleton(op),
                    relation));
        } else if(abstractRelation instanceof RelationDisjunction){
            RelationDisjunction rd = (RelationDisjunction) abstractRelation;
            List<List<ITerm>> abFilters = new ArrayList<>();
            List<RelationTerm> terms = rd.getTerms();
            for(RelationTerm rt: terms){
                List<ITerm> termsList = new ArrayList<>();
                for(AbstractRelation ab: rt.getRelations()){
                    termsList.addAll(createFilter(tableMetadataMap, ab));
                }
                abFilters.add(termsList);
            }
            filters.add(new Disjunction(
                    Collections.singleton(Operations.FILTER_DISJUNCTION),
                    abFilters));
        }
        return filters;
    }

    private void convertSelectSelectors(Relation relation) throws PlanningException {
        Relation currentRelation = relation;
        while(currentRelation.getRightTerm() instanceof RelationSelector){
            currentRelation.setLeftTerm(convertSelectSelector(currentRelation.getLeftTerm()));
            currentRelation.setRightTerm(convertSelectSelector(currentRelation.getRightTerm()));
            currentRelation = ((RelationSelector) currentRelation.getRightTerm()).getRelation();
        }
        currentRelation.setLeftTerm(convertSelectSelector(currentRelation.getLeftTerm()));
        currentRelation.setRightTerm(convertSelectSelector(currentRelation.getRightTerm()));
    }

    private Selector convertSelectSelector(Selector selector) throws PlanningException {
        Selector result = selector;
        if(selector instanceof ExtendedSelectSelector){
            ExtendedSelectSelector extendedSelectSelector = (ExtendedSelectSelector) selector;
            SelectSelector selectSelector = new SelectSelector(selector.getTableName(),
                    extendedSelectSelector.toSQLString());
            LogicalWorkflow innerWorkflow = buildWorkflow(extendedSelectSelector.getSelectValidatedQuery());
            selectSelector.setQueryWorkflow(innerWorkflow);
            result = selectSelector;
        }
        return result;
    }

    /**
     * Add a window operator for streaming queries.
     *
     * @param lastSteps The map associating table names to Project steps
     * @param stmt      The select statement.
     * @return The resulting map of logical steps.
     */
    private Map<String, LogicalStep> addWindow(Map<String, LogicalStep> lastSteps, SelectStatement stmt) {
        Window w = new Window(
                Collections.singleton(Operations.SELECT_WINDOW),
                stmt.getWindow());
        LogicalStep previous = lastSteps.get(stmt.getTableName().getQualifiedName());
        previous.setNextStep(w);
        w.setPrevious(previous);
        lastSteps.put(stmt.getTableName().getQualifiedName(), w);
        return lastSteps;
    }

    /**
     * Add the join logical steps.
     *
     * @param stepMap     The map of last steps after adding filters.
     * @param query       The query.
     * @return The resulting map of logical steps.
     */
    private Map<String, LogicalStep> addJoin(LinkedHashMap<String, LogicalStep> stepMap, SelectValidatedQuery query) {

        for (Join queryJoin: query.getJoinList()) {

            com.stratio.crossdata.common.logicalplan.Join join = getJoin(queryJoin.getType(), query.getStatement().getWindow() != null);

            StringBuilder sb = new StringBuilder();

            if( join.getType() == JoinType.CROSS){

                getCrossJoin(stepMap, queryJoin, join, sb);

            } else {
                for (AbstractRelation ab : queryJoin.getRelations()) {
                    Relation rel = (Relation) ab;

                    if (sb.length() > 0) {
                        sb.append(" & ");
                    }

                    sb.append(rel.getLeftTerm().getTableName().getQualifiedName()).append("$")
                            .append(rel.getRightTerm().getTableName().getQualifiedName());

                    //Attach to input tables path
                    LogicalStep t1 = stepMap.get(rel.getLeftTerm().getSelectorTablesAsString());
                    while (t1.getNextStep() != null) {
                        t1 = t1.getNextStep();
                    }
                    LogicalStep t2 = stepMap.get(rel.getRightTerm().getSelectorTablesAsString());
                    while (t2.getNextStep() != null) {
                        t2 = t2.getNextStep();
                    }

                    addJoinSourceIdentifier(join, rel, t1);
                    addJoinSourceIdentifier(join, rel, t2);

                    List<Relation> or = queryJoin.getOrderedRelations();
                    for (Relation r : or) {
                        if (!join.getJoinRelations().contains(r)) {
                            join.addJoinRelation(r);
                        }
                    }

                    //TODO t1 and t2 can't be null
                    if (t1.getNextStep() != null) {
                        if (!t1.equals(join)) {
                            t1.getNextStep().setNextStep(join);
                        }
                    } else {
                        if (!t1.equals(join)) {
                            t1.setNextStep(join);
                        }
                    }
                    if (t2.getNextStep() != null) {
                        if (!t2.equals(join)) {
                            t2.getNextStep().setNextStep(join);
                        }
                    } else {
                        if (!t2.equals(join)) {
                            t2.setNextStep(join);
                        }
                    }

                    List<LogicalStep> previousSteps = join.getPreviousSteps();
                    if ((!previousSteps.contains(t1)) && (!t1.equals(join))) {
                        join.addPreviousSteps(t1);
                    }
                    if ((!previousSteps.contains(t2)) && (!t2.equals(join))) {
                        join.addPreviousSteps(t2);
                    }
                }
            }
            stepMap.put(sb.toString(), join);
        }
        return stepMap;
    }



    private void addJoinSourceIdentifier(com.stratio.crossdata.common.logicalplan.Join join, Relation rel,
            LogicalStep t1) {
        if (Filter.class.isInstance(t1)) {
            String qualifiedTableName = ((Filter) t1).getRelation().getLeftTerm()
                    .getTableName().getQualifiedName();
            if (!join.getSourceIdentifiers().contains(qualifiedTableName)) {
                join.addSourceIdentifier(qualifiedTableName);
            }
        } else if (Project.class.isInstance(t1)) {
            String qualifiedTableName = ((Project) t1).getTableName().getQualifiedName();
            if (!join.getSourceIdentifiers().contains(qualifiedTableName)) {
                join.addSourceIdentifier(qualifiedTableName);
            }
        } else if (com.stratio.crossdata.common.logicalplan.Join.class.isInstance(t1)) {
            List<String> si = ((com.stratio.crossdata.common.logicalplan.Join) t1).getSourceIdentifiers();
            for (String t : si) {
                if (!join.getSourceIdentifiers().contains(t)) {
                    join.addSourceIdentifier(t);
                }
            }
        } else {
            String qualifiedTableName = rel.getLeftTerm().getTableName().getQualifiedName();
            if (!join.getSourceIdentifiers().contains(qualifiedTableName)) {
                join.addSourceIdentifier(qualifiedTableName);
            }
        }
    }

    private void getCrossJoin(LinkedHashMap<String, LogicalStep> stepMap, Join queryJoin,
            com.stratio.crossdata.common.logicalplan.Join join, StringBuilder sb) {
        Iterator<TableName> tableNameIterator = queryJoin.getTableNames().iterator();
        List<LogicalStep> logicalStepsList = new ArrayList<>();
        while (tableNameIterator.hasNext()) {
            TableName tableName = tableNameIterator.next();
            join.addSourceIdentifier(tableName.getQualifiedName());
            logicalStepsList.add(stepMap.get(tableName.getQualifiedName()));
            if(tableNameIterator.hasNext()){
                sb.append("$");
            }
        }
        for (LogicalStep logicalStep : logicalStepsList) {
            while (logicalStep.getNextStep() != null) {
                logicalStep = logicalStep.getNextStep();
            }
            if (!logicalStep.equals(join)) {
                logicalStep.setNextStep(join);
            }
            join.addPreviousSteps(logicalStep);
        }
    }

    private com.stratio.crossdata.common.logicalplan.Join getJoin(JoinType type, boolean isWindowInc){
        com.stratio.crossdata.common.logicalplan.Join join = null;
        switch(type){
            case INNER:
                //join = isWindowInc ? new com.stratio.crossdata.common.logicalplan.Join(Collections.singleton
                //    (Operations.SELECT_INNER_JOIN_PARTIAL_RESULTS), "innerJoinPR"): new com.stratio.crossdata.common
                //    .logicalplan.Join(Collections.singleton(Operations.SELECT_INNER_JOIN), "innerJoin");
                join = new com.stratio.crossdata.common.logicalplan.Join(Collections.singleton(Operations.SELECT_INNER_JOIN), "innerJoin");
                join.setType(JoinType.INNER);
                break;
            case CROSS:
                //join = isWindowInc ? new com.stratio.crossdata.common.logicalplan.Join(Collections.singleton
                //    (Operations.SELECT_CROSS_JOIN_PARTIAL_RESULTS), "crossJoinPR"): new com.stratio.crossdata.common
                //    .logicalplan.Join(Collections.singleton(Operations.SELECT_CROSS_JOIN), "crossJoin");
                join = new com.stratio.crossdata.common.logicalplan.Join(Collections.singleton(Operations.SELECT_CROSS_JOIN), "crossJoin");
                join.setType(JoinType.CROSS);
                break;
            case LEFT_OUTER:
                //join = isWindowInc ? new com.stratio.crossdata.common.logicalplan.Join(Collections.singleton
                //    (Operations.SELECT_LEFT_OUTER_JOIN_PARTIAL_RESULTS), "leftJoinPR"): new com.stratio.crossdata
                //    .common.logicalplan.Join(Collections.singleton(Operations.SELECT_LEFT_OUTER_JOIN), "leftJoin");
                join = new com.stratio.crossdata.common.logicalplan.Join(Collections.singleton(Operations.SELECT_LEFT_OUTER_JOIN), "leftJoin");
                join.setType(JoinType.LEFT_OUTER);
                break;
            case FULL_OUTER:
                //join = isWindowInc ? new com.stratio.crossdata.common.logicalplan.Join(Collections.singleton
                //    (Operations.SELECT_FULL_OUTER_JOIN_PARTIAL_RESULTS), "fullOuterJoinPR"): new com.stratio
                //    .crossdata.common.logicalplan.Join(Collections.singleton(Operations.SELECT_FULL_OUTER_JOIN),
                //    "fullOuterJoin");
                join = new com.stratio.crossdata.common.logicalplan.Join(Collections.singleton(
                        Operations.SELECT_FULL_OUTER_JOIN), "fullOuterJoin");
                join.setType(JoinType.FULL_OUTER);
                break;
            case RIGHT_OUTER:
                //join = isWindowInc ? new com.stratio.crossdata.common.logicalplan.Join(Collections.singleton
                //    (Operations.SELECT_RIGHT_OUTER_JOIN_PARTIAL_RESULTS), "rightJoinPR"): new com.stratio.crossdata
                //    .common.logicalplan.Join(Collections.singleton(Operations.SELECT_RIGHT_OUTER_JOIN), "rightJoin");
                join = new com.stratio.crossdata.common.logicalplan.Join(Collections.singleton(Operations.SELECT_RIGHT_OUTER_JOIN), "rightJoin");
                join.setType(JoinType.RIGHT_OUTER);
                break;
        }
        return join;
    }


    /**
     * Get a Map associating fully qualified table names with their Project logical step.
     *
     * @param query            The query to be planned.
     * @param tableMetadataMap Map of table metadata.
     * @return A map with the projections.
     */
    protected LinkedHashMap<String, LogicalStep> getProjects(SelectValidatedQuery query,
            Map<String, TableMetadata> tableMetadataMap) {

        LinkedHashMap<String, LogicalStep> projects = new LinkedHashMap<>();
        List<TableName> queryTables = query.getStatement().getFromTables();
        for (TableName tn: queryTables) {
            Project p = generateProject(tn, tableMetadataMap);
            projects.put(tn.getQualifiedName(), p);
        }
        return projects;
    }

    private Project generateProject(TableName tn, Map<String, TableMetadata> tableMetadataMap){
        Project p;
        if (tn.isVirtual()) {
            p = new Project(
                    Collections.singleton(Operations.PROJECT),
                    tn,
                    new ClusterName(Constants.VIRTUAL_NAME));
        } else {
            p = new Project(
                    Collections.singleton(Operations.PROJECT),
                    tn,
                    tableMetadataMap.get(tn.getQualifiedName()).getClusterRef());

        }
        return p;
    }

    /**
     * Generate a select operand.
     *
     * @param selectStatement  The source select statement.
     * @param tableMetadataMap A map with the table metadata indexed by table name.
     * @return A {@link com.stratio.crossdata.common.logicalplan.Select}.
     */
    protected Select generateSelect(SelectStatement selectStatement, Map<String, TableMetadata> tableMetadataMap)
            throws PlanningException {
        LinkedHashMap<Selector, String> aliasMap = new LinkedHashMap<>();
        Map<String,Integer> aliases=new HashMap<>();
        LinkedHashMap<String, ColumnType> typeMap = new LinkedHashMap<>();
        LinkedHashMap<Selector, ColumnType> typeMapFromColumnName = new LinkedHashMap<>();

        Set<Operations> requiredOperations = new HashSet<>();
        requiredOperations.add(Operations.SELECT_OPERATOR);
        for (Selector s: selectStatement.getSelectExpression().getSelectorList()) {
            if (ColumnSelector.class.isInstance(s)) {
                ColumnSelector cs = ColumnSelector.class.cast(s);

                String alias;
                if (cs.getAlias() != null) {
                    alias = cs.getAlias();
                } else {
                    alias = cs.getName().getName();
                }

                if (aliasMap.containsValue(alias)) {
                    if(!aliasMap.containsKey(cs)) {
                        alias = cs.getColumnName().getTableName().getName() + "_" + alias;
                        if (aliases.containsKey(alias)) {
                            aliases.put(alias, aliases.get(alias) + 1);
                            alias = alias + Integer.toString(aliases.get(alias) + 1);
                        } else {
                            aliases.put(alias, 0);
                        }
                    }
                }else{
                    aliases.put(alias,0);
                }
                aliasMap.put(cs, alias);


                ColumnType colType = null;
                //TODO avoid null types
                if (!cs.getTableName().isVirtual()) {
                    colType = tableMetadataMap.get(cs.getSelectorTablesAsString()).getColumns().get(cs.getName())
                            .getColumnType();
                }
                typeMapFromColumnName.put(cs, colType);
                typeMap.put(alias, colType);

            } else if (FunctionSelector.class.isInstance(s)) {
                requiredOperations.add(Operations.SELECT_FUNCTIONS);
                FunctionSelector fs = FunctionSelector.class.cast(s);
                ColumnType ct = null;
                String alias;
                if (fs.getAlias() != null) {
                    alias = fs.getAlias();
                    if (aliasMap.containsValue(alias)) {
                        alias = fs.getTableName().getName() + "_" + fs.getAlias();
                    }

                } else {
                    alias = fs.getFunctionName();
                    if (aliasMap.containsValue(alias)) {
                        alias = fs.getTableName().getName() + "_" + fs.getFunctionName();
                    }
                }
                aliasMap.put(fs, alias);
                typeMapFromColumnName.put(fs, ct);
                typeMap.put(alias, ct);

            } else if (IntegerSelector.class.isInstance(s)) {
                generateLiteralSelect(aliasMap, typeMap, typeMapFromColumnName, s, new ColumnType(DataType.INT));
            } else if (FloatingPointSelector.class.isInstance(s)) {
                generateLiteralSelect(aliasMap, typeMap, typeMapFromColumnName, s, new ColumnType(DataType.DOUBLE));
            } else if (BooleanSelector.class.isInstance(s)) {
                generateLiteralSelect(aliasMap, typeMap, typeMapFromColumnName, s, new ColumnType(DataType.BOOLEAN));
            } else if (StringSelector.class.isInstance(s)) {
                generateLiteralSelect(aliasMap, typeMap, typeMapFromColumnName, s, new ColumnType(DataType.TEXT));
            } else if(RelationSelector.class.isInstance(s)){
                generateLiteralSelect(aliasMap, typeMap, typeMapFromColumnName, s, new ColumnType(DataType.DOUBLE));
            } else if (CaseWhenSelector.class.isInstance(s)) {
                generateCaseWhenSelect(aliasMap, typeMap, typeMapFromColumnName, s);
                requiredOperations.add(Operations.SELECT_CASE_WHEN);
            } else if (NullSelector.class.isInstance(s)) {
                generateLiteralSelect(aliasMap, typeMap, typeMapFromColumnName, s, new ColumnType(DataType.TEXT));
            }  else {
                throw new PlanningException(s.getClass().getCanonicalName() + " is not supported yet.");
            }
        }


        return new Select(requiredOperations, aliasMap, typeMap, typeMapFromColumnName);
    }

    private void generateCaseWhenSelect(LinkedHashMap<Selector, String> aliasMap,
            LinkedHashMap<String, ColumnType> typeMap, LinkedHashMap<Selector, ColumnType> typeMapFromColumnName,
            Selector selector) {

        String alias;
        if (selector.getAlias() != null) {
            alias = selector.getAlias();
            if (aliasMap.containsValue(alias)) {
                alias = selector.getColumnName().getTableName().getName() + "_" + selector.getAlias();
            }

        } else {
            alias = selector.getStringValue();
            selector.setAlias(alias);

        }
        aliasMap.put(selector, alias);
        ColumnType ct = null;
        typeMapFromColumnName.put(selector, ct);
        typeMap.put(alias, ct);
    }

    private void generateLiteralSelect(LinkedHashMap<Selector, String> aliasMap,
            LinkedHashMap<String, ColumnType> typeMap,
            LinkedHashMap<Selector, ColumnType> typeMapFromColumnName, Selector selector, ColumnType columnType)
            throws PlanningException {

        String alias;
        if (selector.getAlias() != null) {
            alias = selector.getAlias();
            if (aliasMap.containsValue(alias)) {
                alias = selector.getColumnName().getTableName().getName() + "_" + selector.getAlias();
            }

        } else {
            alias = selector.getStringValue();
            if(selector instanceof RelationSelector){
                alias = "Column" + (typeMap.size() + 1);
            }
            selector.setAlias(alias);

        }
        aliasMap.put(selector, alias);
        typeMapFromColumnName.put(selector, columnType);
        typeMap.put(alias, columnType);
    }

    private ConnectorMetadata findAnyConnector(ClusterMetadata clusterMetadata, Status status,
            Operations... requiredOperations) throws PlanningException {
        ConnectorMetadata connectorMetadata = null;

        Map<ConnectorName, ConnectorAttachedMetadata> connectorAttachedRefs = clusterMetadata
                .getConnectorAttachedRefs();

        Iterator it = connectorAttachedRefs.keySet().iterator();
        boolean found = false;
        while (it.hasNext() && !found) {
            ConnectorName connectorName = (ConnectorName) it.next();
            connectorMetadata = MetadataManager.MANAGER.getConnector(connectorName);
            if ((connectorMetadata.getStatus() == status) && connectorMetadata.getSupportedOperations()
                    .containsAll(Arrays.asList(requiredOperations))) {
                found = true;
            }
        }
        if (!found) {
            throw new PlanningException("There is no any attached connector supporting: " +
                    System.lineSeparator() + Arrays.toString(requiredOperations));
        }

        return connectorMetadata;
    }

    private String findAnyActorRef(ClusterMetadata clusterMetadata, Status status, Operations... requiredOperations)
            throws PlanningException {
        ConnectorMetadata connectorMetadata = findAnyConnector(clusterMetadata, status, requiredOperations);
        return StringUtils.getAkkaActorRefUri(connectorMetadata.getActorRef(host), false);
    }

}
