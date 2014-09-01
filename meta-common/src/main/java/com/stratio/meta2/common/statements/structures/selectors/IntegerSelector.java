/*
 * Licensed to STRATIO (C) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership.  The STRATIO (C) licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.stratio.meta2.common.statements.structures.selectors;

import com.stratio.meta2.common.data.TableName;

import java.util.Set;

/**
 * Integer value selector.
 */
public class IntegerSelector extends Selector{

  /**
   * The integer value with long precision.
   */
  private final long value;

  /**
   * Class constructor.
   * @param value The integer/long value.
   */
  public IntegerSelector(String value) {
    this.value = Long.valueOf(value);
  }

  /**
   * Get the long value.
   * @return A long value.
   */
  public long getValue() {
    return value;
  }

  @Override
  public SelectorType getType() {
    return SelectorType.INTEGER;
  }

  @Override
  public Set<TableName> getSelectorTables() {
    return null;
  }

  @Override
  public String toString() {
    return Long.toString(value);
  }
}