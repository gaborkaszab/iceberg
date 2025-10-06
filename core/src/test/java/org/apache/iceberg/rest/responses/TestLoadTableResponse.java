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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

public class TestLoadTableResponse extends ResponseWithTableMetadataTest<LoadTableResponse> {

  private static final Map<String, String> CONFIG = ImmutableMap.of("foo", "bar");

  @Override
  public String[] allFieldsFromSpec() {
    return new String[] {"metadata-location", "metadata", "config"};
  }

  @Override
  public LoadTableResponse createExampleInstance() {
    return LoadTableResponse.builder()
        .withTableMetadata(createExampleTableMetadata())
        .addAllConfig(CONFIG)
        .build();
  }

  @Override
  public LoadTableResponse deserialize(String json) throws JsonProcessingException {
    LoadTableResponse resp = mapper().readValue(json, LoadTableResponse.class);
    resp.validate();
    return resp;
  }

  @Test
  public void testFailures() {
    assertThatThrownBy(() -> LoadTableResponse.builder().build())
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Invalid metadata: null");
  }

  @Test
  public void testRoundTripSerdeWithV1TableMetadata() throws Exception {
    String tableMetadataJson = readTableMetadataInputFile("TableMetadataV1Valid.json");
    TableMetadata v1Metadata =
        TableMetadataParser.fromJson(TEST_METADATA_LOCATION, tableMetadataJson);
    // Convert the TableMetadata JSON from the file to an object and then back to JSON so that
    // missing fields
    // are filled in with their default values.
    String json =
        String.format(
            "{\"metadata-location\":\"%s\",\"metadata\":%s,\"config\":{\"foo\":\"bar\"}}",
            TEST_METADATA_LOCATION, TableMetadataParser.toJson(v1Metadata));
    LoadTableResponse resp =
        LoadTableResponse.builder().withTableMetadata(v1Metadata).addAllConfig(CONFIG).build();
    assertRoundTripSerializesEquallyFrom(json, resp);
  }

  @Test
  public void testMissingSchemaType() throws Exception {
    // When the schema type (struct) is missing
    String tableMetadataJson = readTableMetadataInputFile("TableMetadataV1MissingSchemaType.json");
    assertThatThrownBy(
            () -> TableMetadataParser.fromJson(TEST_METADATA_LOCATION, tableMetadataJson))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot parse type from json:");
  }

  @Test
  public void testRoundTripSerdeWithV2TableMetadata() throws Exception {
    String tableMetadataJson = readTableMetadataInputFile("TableMetadataV2Valid.json");
    TableMetadata v2Metadata =
        TableMetadataParser.fromJson(TEST_METADATA_LOCATION, tableMetadataJson);
    // Convert the TableMetadata JSON from the file to an object and then back to JSON so that
    // missing fields are filled in with their default values.
    String json =
        String.format(
            "{\"metadata-location\":\"%s\",\"metadata\":%s,\"config\":{\"foo\":\"bar\"}}",
            TEST_METADATA_LOCATION, TableMetadataParser.toJson(v2Metadata));
    LoadTableResponse resp =
        LoadTableResponse.builder().withTableMetadata(v2Metadata).addAllConfig(CONFIG).build();
    assertRoundTripSerializesEquallyFrom(json, resp);
  }

  @Test
  public void testRoundTripSerdeWithV3TableMetadata() throws Exception {
    String tableMetadataJson = readTableMetadataInputFile("TableMetadataV3ValidMinimal.json");
    TableMetadata v3Metadata =
        TableMetadataParser.fromJson(TEST_METADATA_LOCATION, tableMetadataJson);
    // Convert the TableMetadata JSON from the file to an object and then back to JSON so that
    // missing fields are filled in with their default values.
    String json =
        String.format(
            "{\"metadata-location\":\"%s\",\"metadata\":%s,\"config\":{\"foo\":\"bar\"}}",
            TEST_METADATA_LOCATION, TableMetadataParser.toJson(v3Metadata));
    LoadTableResponse resp =
        LoadTableResponse.builder().withTableMetadata(v3Metadata).addAllConfig(CONFIG).build();
    assertRoundTripSerializesEquallyFrom(json, resp);
  }

  @Test
  public void testCanDeserializeWithoutDefaultValues() throws Exception {
    String metadataJson = readTableMetadataInputFile("TableMetadataV1Valid.json");
    // `config` is missing in the JSON
    String json =
        String.format(
            "{\"metadata-location\":\"%s\",\"metadata\":%s}", TEST_METADATA_LOCATION, metadataJson);
    TableMetadata metadata = TableMetadataParser.fromJson(TEST_METADATA_LOCATION, metadataJson);
    LoadTableResponse actual = deserialize(json);
    LoadTableResponse expected = LoadTableResponse.builder().withTableMetadata(metadata).build();
    assertEquals(actual, expected);
    assertThat(actual.config())
        .as("Deserialized JSON with missing fields should have the default values")
        .isEqualTo(ImmutableMap.of());
  }

  @Override
  public void assertEquals(LoadTableResponse actual, LoadTableResponse expected) {
    assertThat(actual.config())
        .as("Should have the same configuration")
        .isEqualTo(expected.config());
    assertEqualTableMetadata(actual.tableMetadata(), expected.tableMetadata());
    assertThat(actual.metadataLocation())
        .as("Should have the same metadata location")
        .isEqualTo(expected.metadataLocation());
  }

  private String readTableMetadataInputFile(String fileName) throws Exception {
    Path path = Paths.get(getClass().getClassLoader().getResource(fileName).toURI());
    return String.join("", java.nio.file.Files.readAllLines(path));
  }
}
