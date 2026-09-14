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

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.iceberg.formats.DataFileReadBuilder;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

class TestDataFileReadBuilder {
  private static final String DATA_FILE_LOCATION = "s3://bucket/data/file.parquet";
  private static final Map<Integer, PartitionSpec> UNPARTITIONED =
      ImmutableMap.of(PartitionSpec.unpartitioned().specId(), PartitionSpec.unpartitioned());
  private static final ColumnFile COLUMN_FILE =
      ColumnFileStruct.builder()
          .formatVersion(4)
          .fieldIds(List.of(1, 2))
          .location("s3://bucket/data/column-file.parquet")
          .fileFormat(FileFormat.PARQUET)
          .fileSizeInBytes(128L)
          .build();
  private static final Function<String, InputFile> MISSING_FILES = location -> null;

  @Test
  void rejectsDataFilesWithColumnFiles() {
    DataFile file = dataFile(List.of(COLUMN_FILE));

    assertThatThrownBy(() -> DataFileReadBuilder.read(file, StructLike.class, MISSING_FILES))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Reading data files with column files is not supported: " + DATA_FILE_LOCATION);
  }

  @Test
  void rejectsUnknownInputFileLocation() {
    DataFile file = dataFile(null);

    assertThatThrownBy(() -> DataFileReadBuilder.read(file, StructLike.class, MISSING_FILES))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot find input file for location: " + DATA_FILE_LOCATION);
  }

  @Test
  void rejectsInvalidArguments() {
    Function<String, InputFile> provider = location -> null;

    assertThatThrownBy(() -> DataFileReadBuilder.read(null, StructLike.class, provider))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid data file: null");

    assertThatThrownBy(
            () ->
                DataFileReadBuilder.read(
                    dataFile(null), StructLike.class, (Function<String, InputFile>) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid input file provider: null");
  }

  private static DataFile dataFile(List<ColumnFile> columnFiles) {
    TrackedFile file =
        new TrackedFileStruct(
            new TrackingStruct(EntryStatus.ADDED, 42L, 10L, 11L, null, 1000L, null, null, null),
            FileContent.DATA,
            4,
            DATA_FILE_LOCATION,
            FileFormat.PARQUET,
            100L,
            1024L,
            PartitionSpec.unpartitioned().specId(),
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            columnFiles);

    return TrackedFileAdapters.asDataFile(file, UNPARTITIONED);
  }
}
