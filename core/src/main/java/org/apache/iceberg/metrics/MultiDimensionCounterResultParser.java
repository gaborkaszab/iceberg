/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
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
package org.apache.iceberg.metrics;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.JsonUtil;
import org.apache.iceberg.util.Pair;

public class MultiDimensionCounterResultParser {
  private static final String MISSING_FIELD_ERROR_MSG =
      "Cannot parse counter from '%s': Missing field '%s'";
  private static final String UNIT = "unit";
  private static final String TYPE = "type";
  private static final String VALUE = "value";
  private static final String NAME = "name";
  private static final String COUNTERS = "counters";

  private MultiDimensionCounterResultParser() {}

  static <T extends Enum<T>> void toJson(
      String name,
      String genericType,
      MultiDimensionCounterResult<T> multiCounter,
      JsonGenerator gen) throws IOException {
    Preconditions.checkArgument(null != multiCounter, "Invalid counter: null");

    gen.writeFieldName(name);
    gen.writeStartObject();
    gen.writeStringField(UNIT, multiCounter.unit().displayName());
    gen.writeStringField(TYPE, genericType);
    gen.writeArrayFieldStart(COUNTERS);
    for (Map.Entry<T, CounterResult> counterResult : multiCounter.counterResults().entrySet()) {
      gen.writeStartObject();
      gen.writeStringField(NAME, counterResult.getKey().toString());
      gen.writeNumberField(VALUE, counterResult.getValue().value());
      gen.writeEndObject();
    }
    gen.writeEndArray();
    gen.writeEndObject();
  }

  static MultiDimensionCounterResult fromJson(String multiCounterName, JsonNode json) {
    Preconditions.checkArgument(null != json, "Cannot parse counter from null object");
    Preconditions.checkArgument(json.isObject(), "Cannot parse counter from non-object: %s", json);

    if (!json.has(multiCounterName)) {
      return null;
    }

    JsonNode multiCounter = json.get(multiCounterName);
    Preconditions.checkArgument(
        multiCounter.has(UNIT), MISSING_FIELD_ERROR_MSG, multiCounterName, UNIT);
    Preconditions.checkArgument(
        multiCounter.has(TYPE), MISSING_FIELD_ERROR_MSG, multiCounterName, TYPE);
    Preconditions.checkArgument(
        multiCounter.has(COUNTERS), MISSING_FIELD_ERROR_MSG, multiCounterName, COUNTERS);

    String multiCounterUnit = JsonUtil.getString(UNIT, multiCounter);
    JsonNode counters = multiCounter.get(COUNTERS);
    Preconditions.checkState(
        counters.isArray(), "%s should be an array in %s", COUNTERS, multiCounterName);
    ArrayNode countersArray = (ArrayNode) counters;

    Class cls;
    String type = JsonUtil.getString(TYPE, multiCounter);
    try {
      cls = Class.forName(type);
      Preconditions.checkArgument(cls.isEnum(), "%s should be enum in %s", type, multiCounterName);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
    List<Pair<Enum, Long>> counterResults = Lists.newArrayList();
    Iterator<JsonNode> it = countersArray.elements();
    while (it.hasNext()) {
      JsonNode item = it.next();
      Preconditions.checkArgument(item.has(NAME), MISSING_FIELD_ERROR_MSG, COUNTERS, NAME);
      Preconditions.checkArgument(item.has(VALUE), MISSING_FIELD_ERROR_MSG, COUNTERS, VALUE);
      String name = JsonUtil.getString(NAME, item);
      long value = JsonUtil.getLong(VALUE, item);

      counterResults.add(Pair.of(Enum.valueOf(cls, name), value));
    }

    return MultiDimensionCounterResult.of(
        MetricsContext.Unit.fromDisplayName(multiCounterUnit),
        counterResults);
  }
}
