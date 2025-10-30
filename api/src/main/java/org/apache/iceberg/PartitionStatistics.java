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

public interface PartitionStatistics extends StructLike {

  /** Returns the partition of these partition statistics */
  StructLike partition();

  /** Returns the spec ID of the partition of these partition statistics */
  int specId();

  /** Returns the number of data records in the partition */
  long dataRecordCount();

  /** Returns the number of data files in the partition */
  int dataFileCount();

  /** Returns the total size of data files in bytes in the partition */
  long totalDataFileSizeInBytes();

  /** Returns the number of positional delete records in the partition */
  long positionDeleteRecordCount();

  /** Returns the number of positional delete files in the partition */
  int positionDeleteFileCount();

  /** Returns the number of equality delete records in the partition */
  long equalityDeleteRecordCount();

  /** Returns the number of equality delete files in the partition */
  int equalityDeleteFileCount();

  /** Returns the total number of record in the partition */
  Long totalRecords();

  /** Returns the timestamp in milliseconds when the partition was last updated */
  Long lastUpdatedAt();

  /** Returns the ID of the snapshot that last updated this partition */
  Long lastUpdatedSnapshotId();

  /** Returns the number of delete vectors in the partition */
  int dvCount();
}
