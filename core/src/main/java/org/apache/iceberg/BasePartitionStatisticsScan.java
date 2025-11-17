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

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.iceberg.expressions.BoundPredicate;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ExpressionUtil;
import org.apache.iceberg.expressions.ExpressionVisitors;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.expressions.NamedReference;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.expressions.UnboundTerm;
import org.apache.iceberg.expressions.UnboundTransform;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.transforms.Bucket;
import org.apache.iceberg.transforms.Days;
import org.apache.iceberg.transforms.Hours;
import org.apache.iceberg.transforms.Identity;
import org.apache.iceberg.transforms.Months;
import org.apache.iceberg.transforms.Truncate;
import org.apache.iceberg.transforms.VoidTransform;
import org.apache.iceberg.transforms.Years;
import org.apache.iceberg.types.Types;

public class BasePartitionStatisticsScan implements PartitionStatisticsScan {
  private static final Joiner DOT = Joiner.on('.');
  private static final Joiner UNDERSCORE = Joiner.on('_');

  private final Table table;
  private Long snapshotId;
  private Expression filter = Expressions.alwaysTrue();
  private boolean caseSensitive = true;

  public BasePartitionStatisticsScan(Table table) {
    this.table = table;
  }

  @Override
  public PartitionStatisticsScan useSnapshot(long newSnapshotId) {
    Preconditions.checkArgument(
        table.snapshot(newSnapshotId) != null, "Cannot find snapshot with ID %s", newSnapshotId);

    this.snapshotId = newSnapshotId;
    return this;
  }

  @Override
  public PartitionStatisticsScan filter(Expression newFilter) {
    Preconditions.checkArgument(newFilter != null, "Invalid filter: null");

    this.filter = Expressions.and(this.filter, newFilter);
    return this;
  }

  @Override
  public PartitionStatisticsScan caseSensitive(boolean newCaseSensitive) {
    this.caseSensitive = newCaseSensitive;
    return this;
  }

  @Override
  public PartitionStatisticsScan project(Schema newSchema) {
    throw new UnsupportedOperationException("Projection is not supported");
  }

  @Override
  public CloseableIterable<PartitionStatistics> scan() {
    if (snapshotId == null) {
      if (table.currentSnapshot() == null) {
        return CloseableIterable.of(List.of());
      }

      snapshotId = table.currentSnapshot().snapshotId();
    }

    Optional<PartitionStatisticsFile> statsFile =
        table.partitionStatisticsFiles().stream()
            .filter(f -> f.snapshotId() == snapshotId)
            .findFirst();

    if (statsFile.isEmpty()) {
      return CloseableIterable.of(List.of());
    }

    Types.StructType partitionType = Partitioning.partitionType(table);
    Schema schema = PartitionStatsHandler.schema(partitionType, TableUtil.formatVersion(table));

    FileFormat fileFormat = FileFormat.fromFileName(statsFile.get().path());
    Preconditions.checkNotNull(
        fileFormat != null, "Unable to determine format of file: %s", statsFile.get().path());

    CloseableIterable<PartitionStatistics> result =
        InternalData.read(fileFormat, table.io().newInputFile(statsFile.get().path()))
            .project(schema)
            .setRootType(BasePartitionStatistics.class)
            .build();
    if (filter == Expressions.alwaysTrue()) {
      return result;
    }

    Expression filterWithRenamedRefs = renameReferences(partitionType, filter, caseSensitive);

    if (!ExpressionUtil.selectsPartitions(filter, table, caseSensitive)) {
      throw new IllegalArgumentException("Filter isn't aligned with partition columns: " + filter);
    }

    Evaluator evaluator = new Evaluator(schema.asStruct(), filterWithRenamedRefs);
    return CloseableIterable.filter(result, evaluator::eval);
  }

  // TODO gaborkaszab: take care of using caseSensitive + tests

  // TODO gaborkaszab: find a better name. validate and normalize?
  private static Expression renameReferences(
      Types.StructType partitionType, Expression expr, boolean caseSensitive) {
    if (expr == null) {
      return null;
    }

    return ExpressionVisitors.visit(
        expr,
        new ExpressionVisitors.ExpressionVisitor<>() {

          @Override
          public Expression alwaysTrue() {
            return Expressions.alwaysTrue();
          }

          @Override
          public Expression alwaysFalse() {
            return Expressions.alwaysFalse();
          }

          @Override
          public Expression not(Expression result) {
            return Expressions.not(result);
          }

          @Override
          public Expression and(Expression leftResult, Expression rightResult) {
            return Expressions.and(leftResult, rightResult);
          }

          @Override
          public Expression or(Expression leftResult, Expression rightResult) {
            return Expressions.or(leftResult, rightResult);
          }

          @Override
          public <T> Expression predicate(BoundPredicate<T> pred) {
            throw new IllegalStateException("Found already bound predicate: " + pred);
          }

          @Override
          public <T> Expression predicate(UnboundPredicate<T> pred) {
            UnboundTerm<T> term = pred.term();

            if (term instanceof UnboundTransform) {
              UnboundTransform<T, ?> transformTerm = (UnboundTransform<T, ?>) term;

              term = Expressions.ref(partitionColumnNameFrom(transformTerm));
            }

            if (!(term instanceof NamedReference)) {
              throw new IllegalArgumentException("Not supported predicate: " + pred);
            }

            NamedReference<T> nr = (NamedReference<T>) term;

            Types.NestedField field =
                caseSensitive
                    ? partitionType.field(nr.name())
                    : partitionType.caseInsensitiveField(nr.name());
            if (field == null) {
              throw new IllegalArgumentException("Not a partition column: " + nr.name());
            }

            term = Expressions.ref(DOT.join(PartitionStatsHandler.PARTITION_FIELD_NAME, nr.name()));

            // TODO gaborkaszab: support EQ only?
            switch (pred.op()) {
              case IS_NULL:
                return Expressions.isNull(term);
              case NOT_NULL:
                return Expressions.notNull(term);
              case LT:
                return Expressions.lessThan(term, pred.literal().value());
              case LT_EQ:
                return Expressions.lessThanOrEqual(term, pred.literal().value());
              case GT:
                return Expressions.greaterThan(term, pred.literal().value());
              case GT_EQ:
                return Expressions.greaterThanOrEqual(term, pred.literal().value());
              case EQ:
                return Expressions.equal(term, pred.literal().value());
              case NOT_EQ:
                return Expressions.notEqual(term, pred.literal().value());
              case STARTS_WITH:
                return Expressions.startsWith(
                    (UnboundTerm<String>) term, (String) pred.literal().value());
              case IN:
                return Expressions.in(
                    term,
                    pred.literals().stream().map(Literal::value).collect(Collectors.toList()));
              case NOT_IN:
                return Expressions.notIn(
                    term,
                    pred.literals().stream().map(Literal::value).collect(Collectors.toList()));
              default:
                throw new UnsupportedOperationException("Unsupported op: " + pred.op());
            }
          }

          // TODO gaborkaszab: common column naming for transforms sharing PartitionSpec?
          private String partitionColumnNameFrom(UnboundTransform<?, ?> transformTerm) {
            String refName = transformTerm.ref().name();

            if (transformTerm.transform() instanceof Identity) {
              return refName;
            } else if (transformTerm.transform() instanceof Bucket) {
              return UNDERSCORE.join(refName, "bucket");
            } else if (transformTerm.transform() instanceof Truncate) {
              return UNDERSCORE.join(refName, "trunc");
            } else if (transformTerm.transform() instanceof Years) {
              return UNDERSCORE.join(refName, "year");
            } else if (transformTerm.transform() instanceof Months) {
              return UNDERSCORE.join(refName, "month");
            } else if (transformTerm.transform() instanceof Days) {
              return UNDERSCORE.join(refName, "day");
            } else if (transformTerm.transform() instanceof Hours) {
              return UNDERSCORE.join(refName, "hour");
            } else if (transformTerm.transform() instanceof VoidTransform) {
              return UNDERSCORE.join(refName, "null");
            } else {
              throw new IllegalArgumentException("Invalid transform: " + transformTerm.transform());
            }
          }
        });
  }
}
