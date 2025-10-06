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
package org.apache.iceberg.rest.responses;

import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.rest.RESTResponse;

public class CommitTableResponse implements RESTResponse {
  private String metadataLocation;
  private TableMetadata metadata;

  public CommitTableResponse() {
    // Required for Jackson deserialization
  }

  private CommitTableResponse(String metadataLocation, TableMetadata metadata) {
    this.metadataLocation = metadataLocation;
    this.metadata = metadata;
  }

  @Override
  public void validate() {
    Preconditions.checkNotNull(metadataLocation, "Invalid metadataLocation: null");
    Preconditions.checkNotNull(metadata, "Invalid metadata: null");
  }

  public String metadataLocation() {
    return metadataLocation;
  }

  public TableMetadata tableMetadata() {
    return metadata;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("metadataLocation", metadataLocation)
        .add("metadata", metadata)
        .toString();
  }

  public static CommitTableResponse.Builder builder() {
    return new CommitTableResponse.Builder();
  }

  public static class Builder {
    private String metadataLocation;
    private TableMetadata metadata;

    private Builder() {}

    public CommitTableResponse.Builder withTableMetadata(TableMetadata tableMetadata) {
      this.metadataLocation = tableMetadata.metadataFileLocation();
      this.metadata = tableMetadata;
      return this;
    }

    public CommitTableResponse build() {
      Preconditions.checkNotNull(metadataLocation, "Invalid metadataLocation: null");
      Preconditions.checkNotNull(metadata, "Invalid metadata: null");
      return new CommitTableResponse(metadataLocation, metadata);
    }
  }
}
