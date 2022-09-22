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
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

// TODO gaborkaszab: restrict K to be an enum?
// TODO gaborkaszab: Could this also be a generated Immutable class?
public class MultiDimensionCounter<K> {
  private Map<K, Counter> counters = Maps.newConcurrentMap();
  private final MetricsContext metricsContext;
  private final Unit unit;

  public MultiDimensionCounter(MetricsContext metricsContext, Unit unit) {
    this.metricsContext = metricsContext;
    this.unit = unit;
  }

  public void increment(K key, long amount) {
    counters
        .computeIfAbsent(
            key,
            s -> {
              return metricsContext.counter(counterNameForResultDataFilesPerFormat(s));
            })
        .increment(amount);
  }

  public void increment(K key) {
    increment(key, 1);
  }

  // TODO gaborkaszab: make this counter name function pluggable because this way it's not generic.
  public String counterNameForResultDataFilesPerFormat(K key) {
    return "result-" + key.toString().toLowerCase() + "-data-files";
  }

  public Set<K> keys() {
    return counters.keySet();
  }

  public Counter counter(K key) {
    return counters.getOrDefault(key, null);
  }

  public Unit unit() {
    return unit;
  }
}
