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

import java.util.List;
import java.util.Map;
import org.apache.iceberg.metrics.MetricsContext.Unit;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.Pair;
import org.immutables.value.Value;

@Value.Immutable
public abstract class MultiDimensionCounterResult<K> {
  public abstract Map<K, CounterResult> counterResults();

  public abstract Unit unit();

  @Value.Derived
  public long value(K key) {
    if (!counterResults().containsKey(key)) {
      return 0;
    }
    return counterResults().get(key).value();
  }

  @Value.Derived
  public void add(K key, long value) {
    counterResults().put(key, CounterResult.of(unit(), value));
  }

  static <K> MultiDimensionCounterResult fromCounter(MultiDimensionCounter<K> multiCounter) {
    Preconditions.checkArgument(null != multiCounter, "Invalid counter: null");

    Map<K, CounterResult> result = Maps.newHashMap();
    for (K counterKey : multiCounter.keys()) {
      result.put(counterKey, CounterResult.fromCounter(multiCounter.counter(counterKey)));
    }
    return ImmutableMultiDimensionCounterResult.builder()
        .unit(multiCounter.unit())
        .counterResults(result)
        .build();
  }

  static <K> MultiDimensionCounterResult of(Unit unit, K key, long value) {
    Map<K, CounterResult> result = Maps.newHashMap();
    result.put(key, CounterResult.of(unit, value));
    return ImmutableMultiDimensionCounterResult.builder()
        .unit(unit)
        .counterResults(result)
        .build();
  }

  static <K> MultiDimensionCounterResult of(Unit unit, List<Pair<K, Long>> inputs) {
    Map<K, CounterResult> result = Maps.newHashMap();
    for (Pair<K, Long> item : inputs) {
      result.put(item.first(), CounterResult.of(unit, item.second()));
    }
    return ImmutableMultiDimensionCounterResult.builder()
        .unit(unit)
        .counterResults(result)
        .build();
  }
}
