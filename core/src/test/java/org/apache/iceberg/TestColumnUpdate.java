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
package org.apache.iceberg;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.io.File;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.io.TempDir;

public class TestColumnUpdate extends TestBase {
  private static final int FORMAT_VERSION = 4;
  @TempDir private File tablePath;

  @Test
  public void invalidParameters() {
    BaseTable table =
        TestTables.create(tablePath, "invalid_params_test", SCHEMA, SPEC, FORMAT_VERSION);
    table.newAppend().appendFile(FILE_A).commit();

    assertThatThrownBy(() -> table.newColumnUpdate(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid field IDs: null");

    assertThatThrownBy(() -> table.newColumnUpdate(List.of()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid field IDs: empty");

    DataFile updateFile =
        DataFiles.builder(SPEC)
            .withPath("/path/to/data-update.parquet")
            .withFileSizeInBytes(2)
            .withPartitionPath("data_bucket=0")
            .withRecordCount(1)
            .build();

    assertThatThrownBy(() -> table.newColumnUpdate(List.of(1)).addColumnUpdate(null, updateFile))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Base file is null");

    assertThatThrownBy(() -> table.newColumnUpdate(List.of(1)).addColumnUpdate(FILE_A, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Column update file is null");

    // Test mismatched spec IDs
    PartitionSpec specWithDifferentId =
        PartitionSpec.builderFor(SCHEMA).withSpecId(99).bucket("data", BUCKETS_NUMBER).build();
    DataFile updateFileDifferentSpec =
        DataFiles.builder(specWithDifferentId)
            .withPath("/path/to/data-update-different-spec.parquet")
            .withFileSizeInBytes(2)
            .withPartitionPath("data_bucket=0")
            .withRecordCount(1)
            .build();

    assertThatThrownBy(
            () ->
                table.newColumnUpdate(List.of(1)).addColumnUpdate(FILE_A, updateFileDifferentSpec))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Base file spec ID")
        .hasMessageContaining("doesn't match update file spec ID");

    DataFile updateFileMoreRows =
        DataFiles.builder(SPEC)
            .withPath("/path/to/data-update-more-rows.parquet")
            .withFileSizeInBytes(2)
            .withPartitionPath("data_bucket=0")
            .withRecordCount(100) // more rows than FILE_A which has 1
            .build();

    assertThatThrownBy(
            () -> table.newColumnUpdate(List.of(1)).addColumnUpdate(FILE_A, updateFileMoreRows))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Update file can't have more rows than the base file");
  }

  @TestTemplate
  public void unsupportedFormatVersions() {
    assumeThat(formatVersion).isLessThan(4);

    File tableDir = new File(tablePath, "v" + formatVersion);
    BaseTable table =
        TestTables.create(
            tableDir, "old_format_test_v" + formatVersion, SCHEMA, SPEC, formatVersion);
    table.newAppend().appendFile(FILE_A).commit();

    DataFile updateFile =
        DataFiles.builder(SPEC)
            .withPath("/path/to/data-update.parquet")
            .withFileSizeInBytes(2)
            .withPartitionPath("data_bucket=0")
            .withRecordCount(1)
            .build();

    assertThatThrownBy(
            () -> table.newColumnUpdate(List.of(1)).addColumnUpdate(FILE_A, updateFile).commit())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Column updates are supported from V4");
  }

  @Test
  public void updateOnEmptyTable() {
    BaseTable table =
        TestTables.create(tablePath, "empty_table_test", SCHEMA, SPEC, FORMAT_VERSION);

    assertThatThrownBy(
            () -> table.newColumnUpdate(List.of(1)).addColumnUpdate(FILE_A, FILE_B).commit())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Column update is not supported on empty tables");
  }

  @Test
  public void fileToUpdateNotFound() {
    BaseTable table =
        TestTables.create(
            tablePath, "referenced_file_not_found_test", SCHEMA, SPEC, FORMAT_VERSION);
    table.newAppend().appendFile(FILE_A).commit();

    DataFile updateFile =
        DataFiles.builder(SPEC)
            .withPath("/path/to/data-a_update1.parquet")
            .withFileSizeInBytes(2)
            .withPartitionPath("data_bucket=0") // easy way to set partition data for now
            .withRecordCount(1)
            .build();

    assertThatThrownBy(
            () -> table.newColumnUpdate(List.of(1)).addColumnUpdate(FILE_B, updateFile).commit())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Unable to find base data file: /path/to/data-b.parquet");
  }

  @Test
  public void noUpdateFilesProvided() {
    BaseTable table =
        TestTables.create(tablePath, "no_update_files_test", SCHEMA, SPEC, FORMAT_VERSION);
    table.newAppend().appendFile(FILE_A).commit();
    assertThat(table.currentSnapshot().allManifests(table.io())).hasSize(1);
    ManifestFile manifestBeforeUpdate = table.currentSnapshot().allManifests(table.io()).get(0);

    table.newColumnUpdate(List.of(1)).commit();

    assertThat(table.currentSnapshot().allManifests(table.io())).hasSize(1);
    ManifestFile manifestAfterUpdate = table.currentSnapshot().allManifests(table.io()).get(0);
    assertThat(manifestBeforeUpdate).isEqualTo(manifestAfterUpdate);
  }

  @Test
  public void updateColumn() {
    BaseTable table =
        TestTables.create(tablePath, "update_column_test", SCHEMA, SPEC, FORMAT_VERSION);
    table.newAppend().appendFile(FILE_A).appendFile(FILE_B).commit();
    table.newAppend().appendFile(FILE_B).commit();

    List<ManifestFile> manifestsBeforeUpdate = table.currentSnapshot().allManifests(table.io());
    assertThat(manifestsBeforeUpdate).hasSize(2);

    DataFile updateFileA =
        DataFiles.builder(SPEC)
            .withPath("/path/to/data-a_update1.parquet")
            .withFileSizeInBytes(2)
            .withPartitionPath("data_bucket=0") // easy way to set partition data for now
            .withRecordCount(1)
            .build();

    table.newColumnUpdate(List.of(1)).addColumnUpdate(FILE_A, updateFileA).commit();

    List<ManifestFile> manifestsAfterUpdate = table.currentSnapshot().allManifests(table.io());
    assertThat(manifestsAfterUpdate).hasSize(2);

    // TODO gaborkaszab: verify on the content of the manifests. For this, make sure all the
    // possible fields are set.
    // TODO gaborkaszab: for the verification probably read into the pointed avro files too
  }

  @Test
  public void updateColumnWithFieldIdOverlap() {
    BaseTable table =
        TestTables.create(tablePath, "update_column_overlap_test", SCHEMA, SPEC, FORMAT_VERSION);
    table.newAppend().appendFile(FILE_A).appendFile(FILE_B).commit();
    table.newAppend().appendFile(FILE_B).commit();

    List<ManifestFile> manifestsBeforeUpdate = table.currentSnapshot().allManifests(table.io());
    assertThat(manifestsBeforeUpdate).hasSize(2);

    DataFile updateFileA =
        DataFiles.builder(SPEC)
            .withPath("/path/to/data-a_update1.parquet")
            .withFileSizeInBytes(2)
            .withPartitionPath("data_bucket=0") // easy way to set partition data for now
            .withRecordCount(1)
            .build();

    // TODO gaborkaszab: do an overlap
    table.newColumnUpdate(List.of(1, 2)).addColumnUpdate(FILE_A, updateFileA).commit();

    List<ManifestFile> manifestsAfterUpdate = table.currentSnapshot().allManifests(table.io());
    assertThat(manifestsAfterUpdate).hasSize(2);

    // TODO gaborkaszab: verify on the content of the manifests. For this, make sure all the
    // possible fields are set.
    // TODO gaborkaszab: for the verification probably read into the pointed avro files too
  }
}
