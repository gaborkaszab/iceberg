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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.events.CreateSnapshotEvent;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;

/**
 * Implementation of {@link ColumnUpdate} that replaces base data files with column update files in
 * manifests.
 */
public class BaseColumnUpdate extends SnapshotProducer<ColumnUpdate> implements ColumnUpdate {
  private final String tableName;
  private final List<Integer> fieldIds;
  private final SnapshotSummary.Builder summaryBuilder = SnapshotSummary.builder();

  // Mapping from base file path to new file that should replace it
  private final Map<String, DataFile> addedColumnUpdates = Maps.newHashMap();

  // Track new manifests written during apply
  private final List<ManifestFile> newManifests = Lists.newArrayList();

  BaseColumnUpdate(String tableName, TableOperations ops, List<Integer> fieldIds) {
    super(ops);
    Preconditions.checkArgument(fieldIds != null, "Invalid field IDs: null");
    Preconditions.checkArgument(!fieldIds.isEmpty(), "Invalid field IDs: empty");
    this.tableName = tableName;
    this.fieldIds = fieldIds;
  }

  @Override
  protected ColumnUpdate self() {
    return this;
  }

  @Override
  public ColumnUpdate set(String property, String value) {
    summaryBuilder.set(property, value);
    return this;
  }

  // TODO gaborkaszab: is override the best operation?
  @Override
  protected String operation() {
    return DataOperations.OVERWRITE;
  }

  // TODO gaborkaszab: review what this is doing
  @Override
  protected Map<String, String> summary() {
    summaryBuilder.setPartitionSummaryLimit(
        ops()
            .current()
            .propertyAsInt(
                TableProperties.WRITE_PARTITION_SUMMARY_LIMIT,
                TableProperties.WRITE_PARTITION_SUMMARY_LIMIT_DEFAULT));
    return summaryBuilder.build();
  }

  @Override
  public ColumnUpdate addColumnUpdate(DataFile baseFile, DataFile updateFile) {
    Preconditions.checkArgument(baseFile != null, "Base file is null");
    Preconditions.checkArgument(updateFile != null, "Column update file is null");
    Preconditions.checkArgument(
        baseFile.content() == FileContent.DATA, "Base file must be a data file");
    Preconditions.checkArgument(
        updateFile.content() == FileContent.DATA, "Column update file must be a data file");
    Preconditions.checkArgument(
        ops().current().formatVersion() >= 4, "Column updates are supported from V4");
    Preconditions.checkArgument(
        baseFile.specId() == updateFile.specId(),
        "Base file spec ID (%s) doesn't match update file spec ID (%s)",
        baseFile.specId(),
        updateFile.specId());
    Preconditions.checkArgument(
        baseFile.recordCount() >= updateFile.recordCount(),
        "Update file can't have more rows than the base file");

    addedColumnUpdates.put(baseFile.location(), updateFile);

    // Track summary info
    // summaryBuilder.deletedFile(ops().current().spec(baseFile.specId()), baseFile);
    // summaryBuilder.addedFile(ops().current().spec(file.specId()), file);

    return this;
  }

  // TODO gaborkaszab: reject column update if there are eq-deletes
  @Override
  public List<ManifestFile> apply(TableMetadata base, Snapshot snapshot) {
    Preconditions.checkArgument(snapshot != null, "Column update is not supported on empty tables");

    if (addedColumnUpdates.isEmpty()) {
      return snapshot.allManifests(ops().io());
    }

    // TODO gaborkaszab: check if this makes sense
    // TODO gaborkaszab: what is this newManifests used for?
    // Clear any previously written manifests from earlier apply attempts
    if (!newManifests.isEmpty()) {
      newManifests.forEach(file -> deleteFile(file.path()));
      newManifests.clear();
    }

    // TODO gaborkaszab: parallelism by manifest
    // TODO gaborkaszab: do we loose the deleteManifests if we return the data manifests only?
    Set<String> baseFilesToFind = Sets.newConcurrentHashSet();
    baseFilesToFind.addAll(addedColumnUpdates.keySet());
    for (ManifestFile manifest : snapshot.dataManifests(ops().io())) {
      Preconditions.checkState(manifest.content() == ManifestContent.DATA);

      ManifestFile rewrittenManifest =
          applyColumnUpdatesToManifest(
              manifest, base.spec(manifest.partitionSpecId()), baseFilesToFind);
      if (rewrittenManifest != null) {
        newManifests.add(rewrittenManifest);
      }
    }

    if (!baseFilesToFind.isEmpty()) {
      throw new IllegalArgumentException(
          "Unable to find base data file: " + baseFilesToFind.iterator().next());
    }

    return newManifests;
  }

  // TODO gaborkaszab: do I need spec param?
  private ManifestFile applyColumnUpdatesToManifest(
      ManifestFile manifest, PartitionSpec spec, Set<String> baseFilesToFind) {
    ManifestWriter<DataFile> writer = null;
    // Buffer entries seen until we find one that has to be updated
    List<ManifestEntry<DataFile>> bufferedEntries = Lists.newArrayList();
    boolean foundEntryToUpdate = false;

    try (ManifestReader<DataFile> reader = newManifestReader(manifest)) {
      try (CloseableIterable<ManifestEntry<DataFile>> entries = reader.entries()) {
        for (ManifestEntry<DataFile> entry : entries) {
          DataFile existingFile = entry.file();
          DataFile columnUpdateFile = addedColumnUpdates.get(existingFile.location());

          if (!foundEntryToUpdate && columnUpdateFile == null) {
            // Buffer entries until we find the first to apply updates
            bufferedEntries.add(entry);
          } else {
            if (!foundEntryToUpdate) {
              // Found first entry to update. Create writer and flush buffered entries
              foundEntryToUpdate = true;
              writer = newManifestWriter(spec);
              for (ManifestEntry<DataFile> buffered : bufferedEntries) {
                writeEntry(writer, buffered);
              }
              bufferedEntries.clear();
            }
            if (columnUpdateFile == null) {
              writeEntry(writer, entry);
            } else {
              updateEntry(writer, entry, columnUpdateFile);
              baseFilesToFind.remove(existingFile.location());
            }
          }
        }
      }

      if (!foundEntryToUpdate) {
        return manifest;
      }

      writer.close();
      return writer.toManifestFile();
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to rewrite manifest: %s", manifest.path());
    }
  }

  // TODO gaborkaszab: this alters sequence number and file sequence number of the original entry
  private void writeEntry(ManifestWriter<DataFile> writer, ManifestEntry<DataFile> entry) {
    switch (entry.status()) {
      case ADDED:
        writer.addEntry(entry);
        break;
      case EXISTING:
        writer.existing(
            entry.file(),
            entry.snapshotId(),
            entry.dataSequenceNumber(),
            entry.fileSequenceNumber());
        break;
      case DELETED:
        writer.delete(entry.file(), entry.dataSequenceNumber(), entry.fileSequenceNumber());
        break;
      default:
        throw new IllegalArgumentException("Unknown manifest entry status: " + entry.status());
    }
  }

  // TODO gaborkaszab: make sure column update file stats are reflected in manifest list stats too.
  // Is see they have stats on partition fields. Check if update on partition field works, also
  // check if the partition field stats in the manifest list is updated.

  // TODO gaborkaszab: this alters sequence number of the original entry
  private void updateEntry(
      ManifestWriter<DataFile> writer, ManifestEntry<DataFile> entry, DataFile columnUpdateFile) {
    DataFile existingFile = entry.file();

    // TODO gaborkaszab: find and dedup previous column updates
    DataFile fileWithColumnUpdates =
        DataFiles.builder(ops().current().spec())
            .copy(existingFile)
            .withColumnUpdates(fieldIds, columnUpdateFile.location())
            .withMetrics(mergeMetrics(existingFile, columnUpdateFile))
            .build();

    // Create a new ManifestEntry with the same metadata but with the updated file
    GenericManifestEntry<DataFile> newEntry =
        new GenericManifestEntry<>(
            ManifestEntry.getSchema(ops().current().spec().partitionType()).asStruct());

    // TODO gaborkaszab: can I do the below using writeEntry()
    switch (entry.status()) {
      case ADDED:
        newEntry.wrapAppend(entry.snapshotId(), entry.dataSequenceNumber(), fileWithColumnUpdates);
        writer.addEntry(newEntry);
        break;
      case EXISTING:
        newEntry.wrapExisting(
            entry.snapshotId(),
            entry.dataSequenceNumber(),
            entry.fileSequenceNumber(),
            fileWithColumnUpdates);
        writer.existing(newEntry);
        break;
      case DELETED:
        // TODO gaborkaszab: probably no need to add updates to deleted entries. Should we keep them
        // or simply drop?
        newEntry.wrapDelete(
            entry.snapshotId(),
            entry.dataSequenceNumber(),
            entry.fileSequenceNumber(),
            fileWithColumnUpdates);
        writer.delete(newEntry);
        break;
      default:
        throw new IllegalArgumentException("Unknown manifest entry status: " + entry.status());
    }
  }

  // TODO gaborkaszab: add tests
  private Metrics mergeMetrics(DataFile baseFile, DataFile columnUpdateFile) {
    return new Metrics(
        baseFile.recordCount(),
        mergeMetricMaps(baseFile.columnSizes(), columnUpdateFile.columnSizes()),
        mergeMetricMaps(baseFile.valueCounts(), columnUpdateFile.valueCounts()),
        mergeMetricMaps(baseFile.nullValueCounts(), columnUpdateFile.nullValueCounts()),
        mergeMetricMaps(baseFile.nanValueCounts(), columnUpdateFile.nanValueCounts()),
        mergeMetricMaps(baseFile.lowerBounds(), columnUpdateFile.lowerBounds()),
        mergeMetricMaps(baseFile.upperBounds(), columnUpdateFile.upperBounds()));
  }

  private <V> Map<Integer, V> mergeMetricMaps(Map<Integer, V> baseMap, Map<Integer, V> updateMap) {
    if (baseMap == null && updateMap == null) {
      return null;
    }

    Map<Integer, V> merged = Maps.newHashMap();
    if (baseMap != null) {
      merged.putAll(baseMap);
    }

    // Remove the stats for fields that are being updated
    fieldIds.forEach(merged::remove);

    // Only add values from updateMap for fields that are being updated
    if (updateMap != null) {
      fieldIds.forEach(
          fieldId -> {
            if (updateMap.containsKey(fieldId)) {
              merged.put(fieldId, updateMap.get(fieldId));
            }
          });
    }

    return merged.isEmpty() ? null : merged;
  }

  // TODO gaborkaszab: double-check this
  @Override
  public Object updateEvent() {
    long snapshotId = snapshotId();
    Snapshot snapshot = ops().current().snapshot(snapshotId);
    long sequenceNumber = snapshot.sequenceNumber();
    return new CreateSnapshotEvent(
        tableName, operation(), snapshotId, sequenceNumber, snapshot.summary());
  }

  @Override
  protected void cleanUncommitted(Set<ManifestFile> committed) {
    deleteUncommitted(newManifests, committed, true /* clear manifests */);
  }
}
