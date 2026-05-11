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

import java.io.IOException;
import java.util.List;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

class TestColumnFileInfoStruct {

  private static final List<Integer> FIELD_IDS = Lists.newArrayList(1, 2, 3);
  private static final String LOCATION = "s3://bucket/data/column.parquet";

  @Test
  void testFieldAccess() {
    ColumnFileInfoStruct columnFile =
        ColumnFileInfoStruct.builder()
            .fieldIds(FIELD_IDS)
            .location(LOCATION)
            .fileSizeInBytes(1024L)
            .sequenceNumber(7L)
            .build();

    assertThat(columnFile.fieldIds()).containsExactly(1, 2, 3);
    assertThat(columnFile.location()).isEqualTo(LOCATION);
    assertThat(columnFile.fileSizeInBytes()).isEqualTo(1024L);
    assertThat(columnFile.sequenceNumber()).isEqualTo(7L);
  }

  @Test
  void testCopy() {
    ColumnFileInfoStruct columnFile =
        ColumnFileInfoStruct.builder()
            .fieldIds(FIELD_IDS)
            .location(LOCATION)
            .fileSizeInBytes(2048L)
            .sequenceNumber(17L)
            .build();

    ColumnFileInfoStruct copy = columnFile.copy();

    assertThat(copy.fieldIds()).containsExactly(1, 2, 3);
    assertThat(copy.location()).isEqualTo(LOCATION);
    assertThat(copy.fileSizeInBytes()).isEqualTo(2048L);
    assertThat(copy.sequenceNumber()).isEqualTo(17L);

    // verify deep copy
    assertThat(copy.fieldIds()).isNotSameAs(columnFile.fieldIds());
  }

  @Test
  void testNullableFields() {
    ColumnFileInfoStruct columnFile =
        ColumnFileInfoStruct.builder()
            .fieldIds(FIELD_IDS)
            .location(LOCATION)
            .fileSizeInBytes(2048L)
            .build();

    assertThat(columnFile.sequenceNumber()).isNull();
  }

  @Test
  void testStructLikeSize() {
    ColumnFileInfoStruct columnFile = new ColumnFileInfoStruct(ColumnFileInfo.schema());
    assertThat(columnFile.size()).isEqualTo(4);
  }

  @Test
  void testStructLikeGetSet() {
    ColumnFileInfoStruct columnFile = new ColumnFileInfoStruct(ColumnFileInfo.schema());

    columnFile.set(0, Lists.newArrayList(1, 2, 3, 4));
    columnFile.set(1, LOCATION);
    columnFile.set(2, 128L);
    columnFile.set(3, 5L);

    assertThat(columnFile.get(0, List.class)).containsExactly(1, 2, 3, 4);
    assertThat(columnFile.get(1, String.class)).isEqualTo(LOCATION);
    assertThat(columnFile.get(2, Long.class)).isEqualTo(128L);
    assertThat(columnFile.get(3, Long.class)).isEqualTo(5L);
  }

  @Test
  void testProjectedStructLike() {
    Types.StructType projection =
        Types.StructType.of(ColumnFileInfo.LOCATION, ColumnFileInfo.FILE_SIZE_IN_BYTES);

    ColumnFileInfoStruct columnFile = new ColumnFileInfoStruct(projection);
    assertThat(columnFile.size()).isEqualTo(2);

    // projected position 0 maps to internal position 1 (location)
    // projected position 1 maps to internal position 2 (file_size_in_bytes)
    columnFile.set(0, LOCATION);
    columnFile.set(1, 1024L);

    assertThat(columnFile.location()).isEqualTo(LOCATION);
    assertThat(columnFile.fileSizeInBytes()).isEqualTo(1024L);
    assertThat(columnFile.get(0, String.class)).isEqualTo(LOCATION);
    assertThat(columnFile.get(1, Long.class)).isEqualTo(1024L);
  }

  @Test
  void testBuilderValidation() {
    assertThatThrownBy(
            () ->
                ColumnFileInfoStruct.builder()
                    .location(LOCATION)
                    .fileSizeInBytes(1024L)
                    .sequenceNumber(7L)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid field IDs: null");

    assertThatThrownBy(
            () ->
                ColumnFileInfoStruct.builder()
                    .fieldIds(Lists.newArrayList())
                    .location(LOCATION)
                    .fileSizeInBytes(1024L)
                    .sequenceNumber(7L)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid field IDs: empty");

    assertThatThrownBy(
            () ->
                ColumnFileInfoStruct.builder()
                    .fieldIds(FIELD_IDS)
                    .fileSizeInBytes(1024L)
                    .sequenceNumber(7L)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid location: null");

    assertThatThrownBy(
        () ->
            ColumnFileInfoStruct.builder()
                .fieldIds(FIELD_IDS)
                .location("")
                .fileSizeInBytes(1024L)
                .sequenceNumber(7L)
                .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid location: empty");

    assertThatThrownBy(
            () ->
                ColumnFileInfoStruct.builder()
                    .fieldIds(FIELD_IDS)
                    .location(LOCATION)
                    .sequenceNumber(7L)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid file size in bytes: -1 (must be >= 0)");

    assertThatThrownBy(
            () ->
                ColumnFileInfoStruct.builder()
                    .fieldIds(FIELD_IDS)
                    .location(LOCATION)
                    .fileSizeInBytes(1024L)
                    .sequenceNumber(-1L)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid sequence number: -1 (must be >= 0)");
  }

  @Test
  void testJavaSerializationRoundTrip() throws IOException, ClassNotFoundException {
    ColumnFileInfoStruct columnFile =
        ColumnFileInfoStruct.builder()
            .fieldIds(FIELD_IDS)
            .location(LOCATION)
            .fileSizeInBytes(1024L)
            .sequenceNumber(7L)
            .build();

    ColumnFileInfoStruct deserialized = TestHelpers.roundTripSerialize(columnFile);

    assertThat(deserialized.fieldIds()).containsExactly(1, 2, 3);
    assertThat(deserialized.location()).isEqualTo(LOCATION);
    assertThat(deserialized.fileSizeInBytes()).isEqualTo(1024L);
    assertThat(deserialized.sequenceNumber()).isEqualTo(7L);
  }

  @Test
  void testKryoSerializationRoundTrip() throws IOException {
    ColumnFileInfoStruct columnFile =
        ColumnFileInfoStruct.builder()
            .fieldIds(FIELD_IDS)
            .location(LOCATION)
            .fileSizeInBytes(1024L)
            .sequenceNumber(7L)
            .build();

    ColumnFileInfoStruct deserialized = TestHelpers.KryoHelpers.roundTripSerialize(columnFile);

    assertThat(deserialized.fieldIds()).containsExactly(1, 2, 3);
    assertThat(deserialized.location()).isEqualTo(LOCATION);
    assertThat(deserialized.fileSizeInBytes()).isEqualTo(1024L);
    assertThat(deserialized.sequenceNumber()).isEqualTo(7L);
  }
}
