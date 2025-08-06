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

import java.util.List;
import java.util.Map;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.rest.ResponseWithStatusCode;
import org.apache.iceberg.rest.credentials.Credential;

public class LoadTableResponseWithStatusCode extends LoadTableResponse
    implements ResponseWithStatusCode {
  private final int statusCode;

  private LoadTableResponseWithStatusCode(
      String metadataLocation,
      TableMetadata metadata,
      Map<String, String> config,
      List<Credential> credentials,
      int statusCode) {
    super(metadataLocation, metadata, config, credentials);
    this.statusCode = statusCode;
  }

  @Override
  public int statusCode() {
    return statusCode;
  }

  @Override
  public void validate() {}

  public static class Builder extends LoadTableResponse.Builder {
    private final int statusCode;

    public Builder(int statusCode) {
      this.statusCode = statusCode;
    }

    @Override
    public LoadTableResponse build() {
      return new LoadTableResponseWithStatusCode(
          metadataLocation(), metadata(), config(), credentials(), statusCode);
    }
  }
}
