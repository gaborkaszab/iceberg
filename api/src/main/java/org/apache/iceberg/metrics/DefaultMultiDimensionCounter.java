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

import java.util.Map;
import java.util.Set;
import org.apache.iceberg.metrics.MetricsContext.Unit;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

public class DefaultMultiDimensionCounter implements MultiDimensionCounter {
  public static final DefaultMultiDimensionCounter NOOP =
      new DefaultMultiDimensionCounter("NOOP-MultiDimensionCounter", Unit.UNDEFINED, null) {
        @Override
        public String toString() {
          return "NOOP multi-dimension counter";
        }

        @Override
        public void increment(String key) {}

        @Override
        public void increment(String key, long amount) {}

        @Override
        public String name() {
          throw new UnsupportedOperationException("NOOP multi-dimension counter has no name");
        }

        @Override
        public Set<String> keys() {
          throw new UnsupportedOperationException(
              "NOOP multi-dimension counter does not have keys");
        }

        @Override
        public Unit unit() {
          return Unit.UNDEFINED;
        }

        @Override
        public long value(String key) {
          throw new UnsupportedOperationException("NOOP multi-dimension counter has no value");
        }

        @Override
        public boolean isNoop() {
          return true;
        }
      };

  private Map<String, Counter> counters = Maps.newConcurrentMap();
  private final String name;
  private final MetricsContext metricsContext;
  private final Unit unit;

  public DefaultMultiDimensionCounter(String name, Unit unit, MetricsContext metricsContext) {
    Preconditions.checkArgument(null != name, "Invalid name: null");
    Preconditions.checkArgument(null != unit, "Invalid count unit: null");

    this.name = name;
    this.metricsContext = metricsContext;
    this.unit = unit;
  }

  @Override
  public void increment(String key) {
    increment(key, 1);
  }

  @Override
  public void increment(String key, long amount) {
    Preconditions.checkArgument(null != key, "Invalid key: null");
    counters
        .computeIfAbsent(
            key,
            s -> {
              return metricsContext.counter(counterId(s));
            })
        .increment(amount);
  }

  private String counterId(String key) {
    return name.toLowerCase() + "-" + key.toLowerCase();
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public Set<String> keys() {
    return counters.keySet();
  }

  @Override
  public Unit unit() {
    return unit;
  }

  @Override
  public long value(String key) {
    if (!counters.containsKey(key)) {
      return 0;
    }
    return counters.get(key).value();
  }
}
