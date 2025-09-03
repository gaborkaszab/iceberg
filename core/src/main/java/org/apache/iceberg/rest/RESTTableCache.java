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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Ticker;
import java.time.Duration;
import java.util.Map;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.PropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RESTTableCache {
  private static final Logger LOG = LoggerFactory.getLogger(RESTTableCache.class);

  static final class NoopRESTTableCache extends RESTTableCache {
    private NoopRESTTableCache() {
      super(Map.of(), null);
    }

    @Override
    public void invalidate(String sessionId, TableIdentifier identifier) {}

    @Override
    public BaseTable getIfPresent(String sessionId, TableIdentifier identifier) {
      return null;
    }

    @Override
    public void put(String sessionId, TableIdentifier identifier, BaseTable table) {}
  }

  private final long expireAfterWriteMS;
  private final long maxTablesPerSession;
  private final Ticker ticker;

  @SuppressWarnings("checkstyle:VisibilityModifier")
  @VisibleForTesting
  final Cache<String, Cache<TableIdentifier, BaseTable>> perSessionTableCache;

  private RESTTableCache(Map<String, String> properties, Ticker ticker) {
    this.ticker = ticker;

    this.expireAfterWriteMS =
        PropertyUtil.propertyAsLong(
            properties,
            RESTCatalogProperties.TABLE_CACHE_EXPIRE_AFTER_WRITE_MS,
            RESTCatalogProperties.TABLE_CACHE_EXPIRE_AFTER_WRITE_MS_DEFAULT);
    Preconditions.checkArgument(
        this.expireAfterWriteMS >= 0, "Invalid expire after write: negative");

    long numSessions =
        PropertyUtil.propertyAsLong(
            properties,
            RESTCatalogProperties.TABLE_CACHE_MAX_SESSIONS,
            RESTCatalogProperties.TABLE_CACHE_MAX_SESSIONS_DEFAULT);
    Preconditions.checkArgument(numSessions > 0, "Invalid max sessions: zero or negative");

    this.maxTablesPerSession =
        PropertyUtil.propertyAsLong(
            properties,
            RESTCatalogProperties.TABLE_CACHE_MAX_TABLES_PER_SESSION,
            RESTCatalogProperties.TABLE_CACHE_MAX_TABLES_PER_SESSION_DEFAULT);
    Preconditions.checkArgument(
        this.maxTablesPerSession > 0, "Invalid max tables per session: zero or negative");

    this.perSessionTableCache =
        Caffeine.newBuilder()
            .maximumSize(numSessions)
            .removalListener(
                (sessionId, tableCache, cause) ->
                    LOG.debug("Evicted {} from session-level cache ({})", sessionId, cause))
            .recordStats()
            .build();
  }

  private Cache<TableIdentifier, BaseTable> tableCache(String sessionId) {
    return perSessionTableCache.get(
        sessionId,
        id -> {
          Caffeine<Object, Object> builder =
              Caffeine.newBuilder()
                  .maximumSize(maxTablesPerSession)
                  .removalListener(
                      (identifier, table, cause) ->
                          LOG.debug(
                              "Evicted {} from table-level cache for session {} ({})",
                              identifier,
                              sessionId,
                              cause))
                  .recordStats();

          if (expireAfterWriteMS > 0) {
            builder.expireAfterWrite(Duration.ofMillis(expireAfterWriteMS));
            builder.ticker(ticker);
          }

          return builder.build();
        });
  }

  public void invalidate(String sessionId, TableIdentifier identifier) {
    Cache<TableIdentifier, BaseTable> tableCache = perSessionTableCache.getIfPresent(sessionId);
    if (tableCache != null) {
      tableCache.invalidate(identifier);
    }
  }

  public BaseTable getIfPresent(String sessionId, TableIdentifier identifier) {
    Cache<TableIdentifier, BaseTable> tableCache = perSessionTableCache.getIfPresent(sessionId);
    if (tableCache == null) {
      return null;
    }

    return tableCache.getIfPresent(identifier);
  }

  public void put(String sessionId, TableIdentifier identifier, BaseTable table) {
    Preconditions.checkArgument(sessionId != null, "Invalid session ID: null");
    Preconditions.checkArgument(identifier != null, "Invalid table identifier: null");
    Preconditions.checkArgument(table != null, "Invalid table: null");

    tableCache(sessionId).put(identifier, table);
  }

  public static Builder builder(Map<String, String> properties) {
    return new Builder(properties);
  }

  public static class Builder {
    private final boolean enabled;
    private final Map<String, String> properties;
    private Ticker ticker = Ticker.systemTicker();

    private Builder(Map<String, String> properties) {
      this.enabled =
          PropertyUtil.propertyAsBoolean(
              properties,
              RESTCatalogProperties.TABLE_CACHE_ENABLED,
              RESTCatalogProperties.TABLE_CACHE_ENABLED_DEFAULT);

      this.properties = properties;
    }

    @VisibleForTesting
    Builder withTicker(Ticker newTicker) {
      Preconditions.checkArgument(newTicker != null, "Invalid ticker: null");
      this.ticker = newTicker;
      return this;
    }

    public RESTTableCache build() {
      if (!enabled) {
        return new NoopRESTTableCache();
      }

      return new RESTTableCache(properties, ticker);
    }
  }
}
