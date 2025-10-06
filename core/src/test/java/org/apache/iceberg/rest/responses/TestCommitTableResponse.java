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

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.JsonProcessingException;

public class TestCommitTableResponse extends ResponseWithTableMetadataTest<CommitTableResponse> {

  @Override
  public String[] allFieldsFromSpec() {
    return new String[] {"metadata-location", "metadata"};
  }

  @Override
  public CommitTableResponse createExampleInstance() {
    return CommitTableResponse.builder().withTableMetadata(createExampleTableMetadata()).build();
  }

  @Override
  public CommitTableResponse deserialize(String json) throws JsonProcessingException {
    CommitTableResponse resp = mapper().readValue(json, CommitTableResponse.class);
    resp.validate();
    return resp;
  }

  @Override
  public void assertEquals(CommitTableResponse actual, CommitTableResponse expected) {
    assertEqualTableMetadata(actual.tableMetadata(), expected.tableMetadata());
    assertThat(actual.metadataLocation())
        .as("Should have the same metadata location")
        .isEqualTo(expected.metadataLocation());
  }

  // TODO: add test to check mandatory fields
  // TODO: check what other tests to add based on TestLoadTableResponse
}
