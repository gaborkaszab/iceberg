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
package org.apache.iceberg.rest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import com.github.benmanes.caffeine.cache.Cache;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.util.FakeTicker;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestRESTTableCache {
  private static final String SESSION_ID1 = UUID.randomUUID().toString();
  private static final String SESSION_ID2 = UUID.randomUUID().toString();

  private static final Namespace NS = Namespace.of("ns");
  private static final TableIdentifier TABLE_IDENT = TableIdentifier.of(NS, "table_name");

  @TempDir public Path temp;

  @Test
  public void testCacheIsEnabledByDefault() {
    RESTTableCache cache = RESTTableCache.builder(Map.of()).build();

    assertThat(cache).isNotInstanceOf(RESTTableCache.NoopRESTTableCache.class);
  }

  @Test
  public void testInvalidBuildParameters() {
    assertThatThrownBy(() -> RESTTableCache.builder(Map.of()).withTicker(null).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid ticker: null");

    assertThatThrownBy(
            () ->
                RESTTableCache.builder(
                        Map.of(RESTCatalogProperties.TABLE_CACHE_EXPIRE_AFTER_WRITE_MS, "-1"))
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid expire after write: negative");

    assertThatThrownBy(
            () ->
                RESTTableCache.builder(Map.of(RESTCatalogProperties.TABLE_CACHE_MAX_SESSIONS, "0"))
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid max sessions: zero or negative");

    assertThatThrownBy(
            () ->
                RESTTableCache.builder(Map.of(RESTCatalogProperties.TABLE_CACHE_MAX_SESSIONS, "-1"))
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid max sessions: zero or negative");

    assertThatThrownBy(
            () ->
                RESTTableCache.builder(
                        Map.of(RESTCatalogProperties.TABLE_CACHE_MAX_TABLES_PER_SESSION, "0"))
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid max tables per session: zero or negative");

    assertThatThrownBy(
            () ->
                RESTTableCache.builder(
                        Map.of(RESTCatalogProperties.TABLE_CACHE_MAX_TABLES_PER_SESSION, "-1"))
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid max tables per session: zero or negative");
  }

  @Test
  public void testInvalidPutParameters() {
    RESTTableCache cache = RESTTableCache.builder(Map.of()).build();

    BaseTable table = dummyTestTable(TABLE_IDENT);

    assertThatThrownBy(() -> cache.put(null, TABLE_IDENT, table))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid session ID: null");

    assertThatThrownBy(() -> cache.put(SESSION_ID1, null, table))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid table identifier: null");

    assertThatThrownBy(() -> cache.put(SESSION_ID1, TABLE_IDENT, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid table: null");
  }

  @Test
  public void testGetFromEmptyCache() {
    RESTTableCache cache = RESTTableCache.builder(Map.of()).build();

    assertThat(cache.getIfPresent(SESSION_ID1, TABLE_IDENT)).isNull();

    cache.put(SESSION_ID1, TABLE_IDENT, dummyTestTable(TABLE_IDENT));

    assertThat(cache.perSessionTableCache.asMap()).containsOnlyKeys(SESSION_ID1);

    TableIdentifier otherIdent = TableIdentifier.of(NS, "other_table");

    assertThat(cache.getIfPresent(SESSION_ID1, otherIdent)).isNull();
  }

  @Test
  public void testCacheMissOnSessionId() {
    RESTTableCache cache = RESTTableCache.builder(Map.of()).build();

    cache.put(SESSION_ID1, TABLE_IDENT, dummyTestTable(TABLE_IDENT));

    assertThat(cache.perSessionTableCache.asMap()).containsOnlyKeys(SESSION_ID1);
    assertThat(cache.perSessionTableCache.stats().hitCount()).isEqualTo(0);
    assertThat(cache.perSessionTableCache.stats().missCount()).isEqualTo(1);
    assertThat(cache.perSessionTableCache.asMap().get(SESSION_ID1).asMap())
        .containsOnlyKeys(TABLE_IDENT);

    BaseTable table = cache.getIfPresent(SESSION_ID2, TABLE_IDENT);

    assertThat(table).isNull();
    assertThat(cache.perSessionTableCache.stats().hitCount()).isEqualTo(0);
    assertThat(cache.perSessionTableCache.stats().missCount()).isEqualTo(2);
  }

  @Test
  public void testCacheMissOnTableIdent() {
    RESTTableCache cache = RESTTableCache.builder(Map.of()).build();

    cache.put(SESSION_ID1, TABLE_IDENT, dummyTestTable(TABLE_IDENT));

    Cache<TableIdentifier, BaseTable> tableCache =
        cache.perSessionTableCache.asMap().get(SESSION_ID1);
    assertThat(cache.perSessionTableCache.asMap()).containsOnlyKeys(SESSION_ID1);
    assertThat(cache.perSessionTableCache.stats().hitCount()).isEqualTo(0);
    assertThat(tableCache.asMap()).containsOnlyKeys(TABLE_IDENT);
    assertThat(tableCache.stats().missCount()).isEqualTo(0);

    BaseTable table = cache.getIfPresent(SESSION_ID1, TableIdentifier.of(NS, "other_table"));

    assertThat(table).isNull();
    assertThat(cache.perSessionTableCache.stats().hitCount()).isEqualTo(1);
    assertThat(tableCache.stats().hitCount()).isEqualTo(0);
    assertThat(tableCache.stats().missCount()).isEqualTo(1);
  }

  @Test
  public void testCacheHit() {
    RESTTableCache cache = RESTTableCache.builder(Map.of()).build();

    BaseTable table = dummyTestTable(TABLE_IDENT);

    cache.put(SESSION_ID1, TABLE_IDENT, table);

    Cache<TableIdentifier, BaseTable> tableCache =
        cache.perSessionTableCache.asMap().get(SESSION_ID1);

    assertThat(cache.perSessionTableCache.asMap()).containsOnlyKeys(SESSION_ID1);
    assertThat(cache.perSessionTableCache.stats().hitCount()).isEqualTo(0);
    assertThat(tableCache.asMap()).containsOnlyKeys(TABLE_IDENT);
    assertThat(tableCache.stats().hitCount()).isEqualTo(0);
    assertThat(tableCache.stats().missCount()).isEqualTo(0);

    BaseTable tableFromCache = cache.getIfPresent(SESSION_ID1, TABLE_IDENT);

    assertThat(table).isEqualTo(tableFromCache);
    assertThat(cache.perSessionTableCache.stats().hitCount()).isEqualTo(1);
    assertThat(tableCache.stats().hitCount()).isEqualTo(1);
    assertThat(tableCache.stats().missCount()).isEqualTo(0);
  }

  @Test
  public void testMaxSessionsEvictsOlderEntries() {
    RESTTableCache cache =
        RESTTableCache.builder(Map.of(RESTCatalogProperties.TABLE_CACHE_MAX_SESSIONS, "2")).build();

    BaseTable table = dummyTestTable(TABLE_IDENT);

    String sessionId3 = UUID.randomUUID().toString();

    cache.put(SESSION_ID1, TABLE_IDENT, table);
    cache.put(SESSION_ID2, TABLE_IDENT, table);

    assertThat(cache.perSessionTableCache.estimatedSize()).isEqualTo(2);
    assertThat(cache.perSessionTableCache.asMap()).containsOnlyKeys(SESSION_ID1, SESSION_ID2);

    cache.put(sessionId3, TABLE_IDENT, table);

    cache.perSessionTableCache.cleanUp();

    assertThat(cache.perSessionTableCache.estimatedSize()).isEqualTo(2);
    assertThat(cache.perSessionTableCache.asMap()).containsKey(sessionId3);
  }

  @Test
  public void testTablesPerSessionEvictsOlderEntries() {
    RESTTableCache cache =
        RESTTableCache.builder(
                Map.of(RESTCatalogProperties.TABLE_CACHE_MAX_TABLES_PER_SESSION, "2"))
            .build();

    TableIdentifier ident2 = TableIdentifier.of(NS, "ident2");
    TableIdentifier ident3 = TableIdentifier.of(NS, "ident2");

    BaseTable table1 = dummyTestTable(TABLE_IDENT);
    BaseTable table2 = dummyTestTable(ident2);
    BaseTable table3 = dummyTestTable(ident3);

    cache.put(SESSION_ID1, TABLE_IDENT, table1);
    cache.put(SESSION_ID1, ident2, table2);

    assertThat(cache.perSessionTableCache.asMap()).containsOnlyKeys(SESSION_ID1);

    Cache<TableIdentifier, BaseTable> tableCache =
        cache.perSessionTableCache.asMap().get(SESSION_ID1);

    assertThat(tableCache.asMap()).containsOnlyKeys(TABLE_IDENT, ident2);

    cache.put(SESSION_ID1, ident3, table3);

    cache.perSessionTableCache.cleanUp();
    tableCache.cleanUp();

    assertThat(tableCache.estimatedSize()).isEqualTo(2);
    assertThat(tableCache.asMap()).containsKey(ident3);
  }

  @Test
  public void testEvictingTableFromOneSessionKeepsTheSameTableForOtherSessions() {
    RESTTableCache cache =
        RESTTableCache.builder(
                Map.of(RESTCatalogProperties.TABLE_CACHE_MAX_TABLES_PER_SESSION, "1"))
            .build();

    TableIdentifier otherIdent = TableIdentifier.of(NS, "other_ident");

    BaseTable table = dummyTestTable(TABLE_IDENT);
    BaseTable otherTable = dummyTestTable(otherIdent);

    cache.put(SESSION_ID1, TABLE_IDENT, table);
    cache.put(SESSION_ID2, TABLE_IDENT, table);

    assertThat(cache.perSessionTableCache.asMap()).containsOnlyKeys(SESSION_ID1, SESSION_ID2);

    Cache<TableIdentifier, BaseTable> session1TableCache =
        cache.perSessionTableCache.asMap().get(SESSION_ID1);
    Cache<TableIdentifier, BaseTable> session2TableCache =
        cache.perSessionTableCache.asMap().get(SESSION_ID2);

    assertThat(session1TableCache.asMap()).containsOnlyKeys(TABLE_IDENT);
    assertThat(session2TableCache.asMap()).containsOnlyKeys(TABLE_IDENT);

    cache.put(SESSION_ID1, otherIdent, otherTable);

    cache.perSessionTableCache.cleanUp();
    session1TableCache.cleanUp();
    session2TableCache.cleanUp();

    assertThat(cache.perSessionTableCache.estimatedSize()).isEqualTo(2);
    assertThat(session1TableCache.asMap()).containsOnlyKeys(otherIdent);
    assertThat(session2TableCache.asMap()).containsOnlyKeys(TABLE_IDENT);
  }

  @Test
  public void testNoExpiryWhenExpireAfterWritePropertyIsZero() {
    RESTTableCache cache =
        RESTTableCache.builder(
                ImmutableMap.of(RESTCatalogProperties.TABLE_CACHE_EXPIRE_AFTER_WRITE_MS, "0"))
            .build();

    cache.put(SESSION_ID1, TABLE_IDENT, dummyTestTable(TABLE_IDENT));

    Cache<TableIdentifier, BaseTable> tableCache =
        cache.perSessionTableCache.asMap().get(SESSION_ID1);

    assertThat(tableCache.policy().expireAfterWrite()).isNotPresent();
    assertThat(tableCache.policy().expireAfterAccess()).isNotPresent();
  }

  @Test
  public void testTableExpiryAfterTTL() {
    Duration ttl = Duration.ofMinutes(5);
    FakeTicker ticker = new FakeTicker();

    RESTTableCache cache =
        RESTTableCache.builder(
                ImmutableMap.of(
                    RESTCatalogProperties.TABLE_CACHE_EXPIRE_AFTER_WRITE_MS,
                    Long.toString(ttl.toMillis())))
            .withTicker(ticker)
            .build();

    cache.put(SESSION_ID1, TABLE_IDENT, dummyTestTable(TABLE_IDENT));

    Cache<TableIdentifier, BaseTable> tableCache =
        cache.perSessionTableCache.asMap().get(SESSION_ID1);

    assertThat(tableCache.policy().expireAfterWrite().get().ageOf(TABLE_IDENT))
        .isPresent()
        .get()
        .isEqualTo(Duration.ZERO);
    assertThat(tableCache.policy().expireAfterAccess()).isNotPresent();

    ticker.advance(ttl.dividedBy(2));

    assertThat(tableCache.asMap()).containsOnlyKeys(TABLE_IDENT);
    assertThat(tableCache.policy().expireAfterWrite().get().ageOf(TABLE_IDENT))
        .isPresent()
        .get()
        .isEqualTo(ttl.dividedBy(2));

    ticker.advance(ttl.dividedBy(2).plus(Duration.ofSeconds(10)));

    assertThat(tableCache.asMap()).doesNotContainKey(TABLE_IDENT);
  }

  @Test
  public void testTableAccessDoesNotResetAge() {
    FakeTicker ticker = new FakeTicker();

    RESTTableCache cache = RESTTableCache.builder(Map.of()).withTicker(ticker).build();

    cache.put(SESSION_ID1, TABLE_IDENT, dummyTestTable(TABLE_IDENT));

    Cache<TableIdentifier, BaseTable> tableCache =
        cache.perSessionTableCache.asMap().get(SESSION_ID1);

    assertThat(tableCache.policy().expireAfterWrite().get().ageOf(TABLE_IDENT))
        .isPresent()
        .get()
        .isEqualTo(Duration.ZERO);
    assertThat(tableCache.policy().expireAfterAccess()).isNotPresent();

    ticker.advance(Duration.ofMinutes(1));

    assertThat(tableCache.stats().hitCount()).isEqualTo(0);
    assertThat(tableCache.asMap()).containsOnlyKeys(TABLE_IDENT);
    assertThat(tableCache.policy().expireAfterWrite().get().ageOf(TABLE_IDENT))
        .isPresent()
        .get()
        .isEqualTo(Duration.ofMinutes(1));

    cache.getIfPresent(SESSION_ID1, TABLE_IDENT);

    assertThat(tableCache.stats().hitCount()).isEqualTo(1);
    assertThat(tableCache.policy().expireAfterWrite().get().ageOf(TABLE_IDENT))
        .isPresent()
        .get()
        .isEqualTo(Duration.ofMinutes(1));
  }

  @Test
  public void testTableWriteResetsAge() {
    FakeTicker ticker = new FakeTicker();

    RESTTableCache cache = RESTTableCache.builder(Map.of()).withTicker(ticker).build();

    cache.put(SESSION_ID1, TABLE_IDENT, dummyTestTable(TABLE_IDENT));

    Cache<TableIdentifier, BaseTable> tableCache =
        cache.perSessionTableCache.asMap().get(SESSION_ID1);

    assertThat(tableCache.policy().expireAfterWrite().get().ageOf(TABLE_IDENT))
        .isPresent()
        .get()
        .isEqualTo(Duration.ZERO);

    ticker.advance(Duration.ofMinutes(1));

    assertThat(tableCache.asMap()).containsOnlyKeys(TABLE_IDENT);
    assertThat(tableCache.policy().expireAfterWrite().get().ageOf(TABLE_IDENT))
        .isPresent()
        .get()
        .isEqualTo(Duration.ofMinutes(1));

    cache.put(SESSION_ID1, TABLE_IDENT, dummyTestTable(TABLE_IDENT));

    assertThat(tableCache.policy().expireAfterWrite().get().ageOf(TABLE_IDENT))
        .isPresent()
        .get()
        .isEqualTo(Duration.ZERO);
  }

  @Test
  public void testDisabledCache() {
    RESTTableCache cache =
        RESTTableCache.builder(Map.of(RESTCatalogProperties.TABLE_CACHE_ENABLED, "false")).build();

    assertThat(cache).isInstanceOf(RESTTableCache.NoopRESTTableCache.class);

    cache.put(SESSION_ID1, TABLE_IDENT, dummyTestTable(TABLE_IDENT));
    cache.put(null, null, null);

    assertThat(cache.perSessionTableCache.estimatedSize()).isEqualTo(0);

    cache.getIfPresent(SESSION_ID1, TABLE_IDENT);
    cache.getIfPresent(null, null);

    assertThat(cache.perSessionTableCache.estimatedSize()).isEqualTo(0);

    cache.invalidate(SESSION_ID1, TABLE_IDENT);
    cache.invalidate(null, null);

    assertThat(cache.perSessionTableCache.estimatedSize()).isEqualTo(0);
  }

  private BaseTable dummyTestTable(TableIdentifier ident) {
    return new BaseTable(mock(TableOperations.class), ident.toString());
  }
}
