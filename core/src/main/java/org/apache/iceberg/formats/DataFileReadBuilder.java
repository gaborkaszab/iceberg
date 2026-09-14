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
package org.apache.iceberg.formats;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * Reads a {@link DataFile}, including the column files it references.
 *
 * <p>This builder is the entry point for reading a data file. It exposes the same configuration as
 * {@link ReadBuilder} and turns the logical data file into the physical reads needed to produce its
 * rows. A data file without column files is read by a single {@link ReadBuilder} obtained from
 * {@link FormatModelRegistry}.
 *
 * <p>The physical files are resolved by location through the function supplied by the caller, so
 * the builder needs neither a {@code FileIO} nor an encryption manager. Readers that already
 * maintain a location to {@link InputFile} mapping for a task group can supply its lookup directly.
 *
 * @param <D> the output data type produced by the reader
 * @param <S> the type of the schema for the output data type
 */
public class DataFileReadBuilder<D, S> implements ReadBuilder<D, S> {
  private final ReadBuilder<D, S> delegate;

  private DataFileReadBuilder(ReadBuilder<D, S> delegate) {
    this.delegate = delegate;
  }

  /**
   * Returns a builder reading the given data file.
   *
   * @param file the data file to read
   * @param type the output type
   * @param inputFiles resolves the physical files of the read by location
   * @param <D> the type of data records the reader will produce
   * @param <S> the type of the output schema for the reader
   * @return a configurable builder for the given data file
   */
  public static <D, S> DataFileReadBuilder<D, S> read(
      DataFile file, Class<? extends D> type, Function<String, InputFile> inputFiles) {
    Preconditions.checkArgument(file != null, "Invalid data file: null");
    Preconditions.checkArgument(inputFiles != null, "Invalid input file provider: null");

    List<?> columnFiles = file.columnFiles();
    if (columnFiles != null && !columnFiles.isEmpty()) {
      throw new UnsupportedOperationException(
          "Reading data files with column files is not supported: " + file.location());
    }

    InputFile inputFile = inputFiles.apply(file.location());
    Preconditions.checkArgument(
        inputFile != null, "Cannot find input file for location: %s", file.location());

    return new DataFileReadBuilder<>(
        FormatModelRegistry.readBuilder(file.format(), type, inputFile));
  }

  @Override
  public DataFileReadBuilder<D, S> split(long start, long length) {
    delegate.split(start, length);
    return this;
  }

  @Override
  public DataFileReadBuilder<D, S> project(Schema schema) {
    delegate.project(schema);
    return this;
  }

  @Override
  public DataFileReadBuilder<D, S> engineProjection(S schema) {
    delegate.engineProjection(schema);
    return this;
  }

  @Override
  public DataFileReadBuilder<D, S> caseSensitive(boolean caseSensitive) {
    delegate.caseSensitive(caseSensitive);
    return this;
  }

  @Override
  public DataFileReadBuilder<D, S> filter(Expression filter) {
    delegate.filter(filter);
    return this;
  }

  @Override
  public DataFileReadBuilder<D, S> set(String key, String value) {
    delegate.set(key, value);
    return this;
  }

  @Override
  public DataFileReadBuilder<D, S> reuseContainers() {
    delegate.reuseContainers();
    return this;
  }

  @Override
  public DataFileReadBuilder<D, S> recordsPerBatch(int rowsPerBatch) {
    delegate.recordsPerBatch(rowsPerBatch);
    return this;
  }

  @Override
  public DataFileReadBuilder<D, S> idToConstant(Map<Integer, ?> idToConstant) {
    delegate.idToConstant(idToConstant);
    return this;
  }

  @Override
  public DataFileReadBuilder<D, S> withNameMapping(NameMapping nameMapping) {
    delegate.withNameMapping(nameMapping);
    return this;
  }

  @Override
  public CloseableIterable<D> build() {
    return delegate.build();
  }
}
