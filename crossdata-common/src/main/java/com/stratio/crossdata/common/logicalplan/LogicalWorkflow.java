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

import java.io.Serializable;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Workflow defining the steps to be executed to retrieve the requested data. Notice that a workflow may contain several
 * entry points (e.g., for a JOIN operation). The list of initial steps contains Project operations that should be
 * navigated using the getNextStep to determine the next step.
 */
public class LogicalWorkflow implements Serializable {

    private static final long serialVersionUID = -4543756106145134702L;
    /**
     * List of initial steps. All initial steps MUST be Project operations.
     */
    private final List<LogicalStep> initialSteps;

    /**
     * Last logical step.
     */
    private LogicalStep lastStep = null;

    private int pagination = 0;

    private String sqlDirectQuery;

    /**
     * Workflow constructor.
     *
     * @param initialSteps The list of initial steps.
     * @param lastStep The last logical step.
     * @param pagination The size of the pagination.
     */
    public LogicalWorkflow(List<LogicalStep> initialSteps, LogicalStep lastStep, int pagination) {
        this.initialSteps = initialSteps;
        this.lastStep = lastStep;
        this.pagination = pagination;
    }

    /**
     * Workflow constructor.
     * 
     * @param initialSteps
     *            The list of initial steps.
     */
    public LogicalWorkflow(List<LogicalStep> initialSteps) {
        this.initialSteps = initialSteps;
    }

    /**
     * Get the list of initial steps.
     * 
     * @return The list of initial steps.
     */
    public List<LogicalStep> getInitialSteps() {
        return initialSteps;
    }

    /**
     * Get the last step of the workflow.
     * 
     * @return A {@link com.stratio.crossdata.common.logicalplan.LogicalStep}.
     */
    public LogicalStep getLastStep() {
        if (lastStep == null && initialSteps.size() > 0) {
            // Find last step.
            LogicalStep last = initialSteps.get(0);
            while (last.getNextStep() != null) {
                last = last.getNextStep();
            }
            this.lastStep = last;
        }
        return lastStep;
    }

    /**
     * Set the last step of the workflow.
     * 
     * @param lastStep
     *            The last logical step.
     */
    public void setLastStep(LogicalStep lastStep) {
        this.lastStep = lastStep;
    }

    public int getPagination() {
        return pagination;
    }

    public void setPagination(int pagination) {
        this.pagination = pagination;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("LogicalWorkflow").append(System.lineSeparator());

        Set<LogicalStep> pending = new HashSet<>();
        LogicalStep step = null;
        // Print initial PROJECT paths
        for (LogicalStep initial : initialSteps) {
            step = initial;
            sb.append(step).append(System.lineSeparator());
            step = step.getNextStep();
            while (step != null) {
                if (UnionStep.class.isInstance(step)) {
                    pending.add(step);
                    step = null;
                } else {
                    sb.append("\t").append(step).append(System.lineSeparator());
                    step = step.getNextStep();
                }

            }

        }

        // Print union paths.
        for (LogicalStep union : pending) {
            step = union;
            sb.append(step).append(System.lineSeparator());
            step = step.getNextStep();
            while (step != null) {
                sb.append("\t").append(step).append(System.lineSeparator());
                step = step.getNextStep();
            }
        }

        return sb.toString();
    }

    /**
     * Get the sql query in sql 92 standard.
     * @return A String with the query.
     */
    public String getSqlDirectQuery() {
        return sqlDirectQuery;
    }

    /**
     * Set the sql query in a sql 92  standard format.
     * @param sqlDirectQuery The query.
     */
    public void setSqlDirectQuery(String sqlDirectQuery) {
        this.sqlDirectQuery = sqlDirectQuery;
    }
}
