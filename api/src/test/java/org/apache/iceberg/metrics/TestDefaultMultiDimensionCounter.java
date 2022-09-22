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

import org.apache.iceberg.metrics.MetricsContext.Unit;
import org.assertj.core.api.Assertions;
import org.junit.Test;

public class TestDefaultMultiDimensionCounter {
  static final String COUNTER_NAME1 = "counter-name1";
  static final String COUNTER_NAME2 = "counter-name2";
  static final String KEY1 = "key1";

  @Test
  public void nullCheck() {
    MetricsContext metricsContext = new DefaultMetricsContext();
    Assertions.assertThatThrownBy(() -> metricsContext.multiCounter(null, Unit.COUNT))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid name: null");

    Assertions.assertThatThrownBy(() -> new DefaultMetricsContext().multiCounter("test", null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid count unit: null");
  }

  @Test
  public void noop() {
    Assertions.assertThat(DefaultMultiDimensionCounter.NOOP.unit()).isEqualTo(Unit.UNDEFINED);
    Assertions.assertThat(DefaultMultiDimensionCounter.NOOP.isNoop()).isTrue();
    Assertions.assertThatThrownBy(() -> DefaultMultiDimensionCounter.NOOP.value("key"))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("NOOP multi-dimension counter has no value");
    Assertions.assertThatThrownBy(DefaultMultiDimensionCounter.NOOP::name)
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("NOOP multi-dimension counter has no name");
    Assertions.assertThatThrownBy(DefaultMultiDimensionCounter.NOOP::keys)
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("NOOP multi-dimension counter does not have keys");
  }

  @Test
  public void count() {
    MetricsContext metricsContext = new DefaultMetricsContext();
    MultiDimensionCounter counter1 =
        new DefaultMultiDimensionCounter(COUNTER_NAME1, Unit.BYTES, metricsContext);
    Assertions.assertThat(counter1.name()).isEqualTo(COUNTER_NAME1);
    Assertions.assertThat(counter1.unit()).isEqualTo(MetricsContext.Unit.BYTES);
    Assertions.assertThat(counter1.isNoop()).isFalse();

    counter1.increment(KEY1);
    counter1.increment(KEY1, 2L);
    Assertions.assertThat(counter1.value(KEY1)).isEqualTo(3L);

    counter1.increment(KEY1.toUpperCase(), 10L);
    Assertions.assertThat(counter1.value(KEY1)).isEqualTo(3L);
    Assertions.assertThat(counter1.value(KEY1.toUpperCase())).isEqualTo(10L);

    // Test counter with different name but using same keys
    MultiDimensionCounter counter2 =
        new DefaultMultiDimensionCounter(COUNTER_NAME2, Unit.BYTES, metricsContext);
    Assertions.assertThat(counter2.name()).isEqualTo(COUNTER_NAME2);
    counter2.increment(KEY1, 100L);
    Assertions.assertThat(counter2.value(KEY1)).isEqualTo(100L);
    Assertions.assertThat(counter1.value(KEY1)).isEqualTo(3L);

    // Test counter with same name and same keys
    MultiDimensionCounter counter3 =
        new DefaultMultiDimensionCounter(COUNTER_NAME1, Unit.BYTES, metricsContext);
    Assertions.assertThat(counter3.name()).isEqualTo(COUNTER_NAME1);
    counter3.increment(KEY1, 1000L);
    Assertions.assertThat(counter3.value(KEY1)).isEqualTo(1000L);
    Assertions.assertThat(counter1.value(KEY1)).isEqualTo(3L);

    Assertions.assertThatThrownBy(() -> counter1.increment(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid key: null");
  }
}
