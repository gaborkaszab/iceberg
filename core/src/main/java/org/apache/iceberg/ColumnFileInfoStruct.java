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

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import org.apache.iceberg.avro.SupportsIndexProjection;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ArrayUtil;

/** Mutable {@link StructLike} implementation of {@link ColumnFileInfo}. */
class ColumnFileInfoStruct extends SupportsIndexProjection implements ColumnFileInfo, Serializable {
  private static final Types.StructType BASE_TYPE =
      Types.StructType.of(
          ColumnFileInfo.FIELD_IDS,
          ColumnFileInfo.LOCATION,
          ColumnFileInfo.FILE_SIZE_IN_BYTES,
          ColumnFileInfo.SEQUENCE_NUMBER);

  private int[] fieldIds = null;
  private String location = null;
  private long fileSizeInBytes = -1L;
  private Long sequenceNumber = null;

  /** Used by internal readers to instantiate this class with a projection schema. */
  ColumnFileInfoStruct(Types.StructType projection) {
    super(BASE_TYPE, projection);
  }

  private ColumnFileInfoStruct(
      int[] fieldIds, String location, long fileSizeInBytes, Long sequenceNumber) {
    super(BASE_TYPE, BASE_TYPE);
    this.fieldIds = fieldIds;
    this.location = location;
    this.fileSizeInBytes = fileSizeInBytes;
    this.sequenceNumber = sequenceNumber;
  }

  /** Copy constructor. */
  private ColumnFileInfoStruct(ColumnFileInfoStruct toCopy) {
    super(toCopy);
    this.fieldIds =
        toCopy.fieldIds != null ? Arrays.copyOf(toCopy.fieldIds, toCopy.fieldIds.length) : null;
    this.location = toCopy.location;
    this.fileSizeInBytes = toCopy.fileSizeInBytes;
    this.sequenceNumber = toCopy.sequenceNumber;
  }

  @Override
  public List<Integer> fieldIds() {
    return fieldIds != null ? ArrayUtil.toUnmodifiableIntList(fieldIds) : null;
  }

  @Override
  public String location() {
    return location;
  }

  @Override
  public long fileSizeInBytes() {
    return fileSizeInBytes;
  }

  @Override
  public Long sequenceNumber() {
    return sequenceNumber;
  }

  @Override
  public ColumnFileInfoStruct copy() {
    return new ColumnFileInfoStruct(this);
  }

  @Override
  protected <T> T internalGet(int pos, Class<T> javaClass) {
    return javaClass.cast(getByPos(pos));
  }

  private Object getByPos(int pos) {
    switch (pos) {
      case 0:
        return fieldIds();
      case 1:
        return location;
      case 2:
        return fileSizeInBytes;
      case 3:
        return sequenceNumber;
      default:
        throw new UnsupportedOperationException("Unknown field ordinal: " + pos);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  protected <T> void internalSet(int pos, T value) {
    switch (pos) {
      case 0:
        this.fieldIds = ArrayUtil.toIntArray((List<Integer>) value);
        break;
      case 1:
        // always coerce to String for Serializable
        this.location = value.toString();
        break;
      case 2:
        this.fileSizeInBytes = (Long) value;
        break;
      case 3:
        this.sequenceNumber = (Long) value;
        break;
      default:
        // ignore the object, it must be from a newer version of the format
    }
  }

  static Builder builder() {
    return new Builder();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("field_ids", fieldIds == null ? "null" : fieldIds())
        .add("location", location)
        .add("file_size_in_bytes", fileSizeInBytes)
        .add("sequence_number", sequenceNumber == null ? "null" : sequenceNumber)
        .toString();
  }

  static class Builder {
    private int[] fieldIds = null;
    private String location = null;
    private long fileSizeInBytes = -1L;
    private Long sequenceNumber = null;

    Builder fieldIds(List<Integer> ids) {
      this.fieldIds = ids != null ? ArrayUtil.toIntArray(ids) : null;
      return this;
    }

    Builder location(String columnFileLocation) {
      this.location = columnFileLocation;
      return this;
    }

    Builder fileSizeInBytes(long size) {
      this.fileSizeInBytes = size;
      return this;
    }

    Builder sequenceNumber(Long sequence) {
      this.sequenceNumber = sequence;
      return this;
    }

    ColumnFileInfoStruct build() {
      Preconditions.checkArgument(fieldIds != null, "Invalid field IDs: null");
      Preconditions.checkArgument(fieldIds.length > 0, "Invalid field IDs: empty");
      Preconditions.checkArgument(location != null, "Invalid location: null");
      Preconditions.checkArgument(!location.isEmpty(), "Invalid location: empty");
      Preconditions.checkArgument(
          fileSizeInBytes >= 0, "Invalid file size in bytes: %s (must be >= 0)", fileSizeInBytes);
      Preconditions.checkArgument(
          sequenceNumber == null || sequenceNumber >= 0,
          "Invalid sequence number: %s (must be >= 0)",
          sequenceNumber);
      return new ColumnFileInfoStruct(fieldIds, location, fileSizeInBytes, sequenceNumber);
    }
  }
}
