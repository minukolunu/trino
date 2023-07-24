/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.trino.spi.connector.*;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.predicate.TupleDomain;

import java.util.*;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class MilvusConnectorMetadata implements ConnectorMetadata {

  private final MilvusClient milvusClient;

  @Inject
  public MilvusConnectorMetadata(MilvusClient milvusClient) {
    this.milvusClient = milvusClient;
  }

  public List<String> listSchemaNames() {
    return ImmutableList.copyOf(milvusClient.getSchemaNames());
  }

  @Override
  public List<String> listSchemaNames(ConnectorSession session) {
    return ImmutableList.copyOf(milvusClient.getSchemaNames());
  }

  @Override
  public Optional<TableFunctionApplicationResult<ConnectorTableHandle>> applyTableFunction(
      ConnectorSession session, ConnectorTableFunctionHandle handle) {
    return ConnectorMetadata.super.applyTableFunction(session, handle);
  }

  @Override
  public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName) {
    if (schemaName.isPresent()) {
      return milvusClient.getTableNames(schemaName.get()).stream()
          .map(table -> new SchemaTableName(schemaName.get(), table))
          .collect(Collectors.toList());
    }
    throw new RuntimeException("Schema name is mandatory");
  }

  @Override
  public Optional<LimitApplicationResult<ConnectorTableHandle>> applyLimit(
      ConnectorSession session, ConnectorTableHandle handle, long limit) {
    return ConnectorMetadata.super.applyLimit(session, handle, limit);
  }

  @Override
  public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName) {
    return new MilvusTableHandle(
        tableName.getSchemaName(), tableName.getTableName(), TupleDomain.all());
  }

  @Override
  public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(
      ConnectorSession session, SchemaTablePrefix prefix) {

    requireNonNull(prefix, "prefix is null");
    ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
    for (SchemaTableName tableName : listTables(session, prefix.getSchema())) {
      ConnectorTableMetadata tableMetadata = getTableMetadata(tableName);
      if (tableMetadata != null) {
        columns.put(tableName, tableMetadata.getColumns());
      }
    }

    return columns.buildOrThrow();
  }

  private ConnectorTableMetadata getTableMetadata(SchemaTableName tableName) {
    MilvusTable table = milvusClient.getTable(tableName.getSchemaName(), tableName.getTableName());
    if (table == null) {
      return null;
    }
    return new ConnectorTableMetadata(tableName, table.getColumnsMetadata());
  }

  @Override
  public ConnectorTableMetadata getTableMetadata(
      ConnectorSession session, ConnectorTableHandle table) {
    return getTableMetadata(((MilvusTableHandle) table).toSchemaTableName());
  }

  @Override
  public Map<String, ColumnHandle> getColumnHandles(
      ConnectorSession session, ConnectorTableHandle tableHandle) {
    MilvusTableHandle milvusTableHandle = (MilvusTableHandle) tableHandle;

    MilvusTable table =
        milvusClient.getTable(milvusTableHandle.getSchemaName(), milvusTableHandle.getTableName());
    if (table == null) {
      throw new TableNotFoundException(milvusTableHandle.toSchemaTableName());
    }

    ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
    int index = 0;
    for (ColumnMetadata column : table.getColumnsMetadata()) {
      columnHandles.put(
          column.getName(),
          new MilvusColumnHandle(column.getName(), column.getType(), column.getExtraInfo(), index));
      index++;
    }
    return columnHandles.buildOrThrow();
  }

  @Override
  public ColumnMetadata getColumnMetadata(
      ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle) {
    return ((MilvusColumnHandle) columnHandle).getColumnMetadata();
  }

  @Override
  public Optional<TopNApplicationResult<ConnectorTableHandle>> applyTopN(
      ConnectorSession session,
      ConnectorTableHandle handle,
      long topNCount,
      List<SortItem> sortItems,
      Map<String, ColumnHandle> assignments) {
    return ConnectorMetadata.super.applyTopN(session, handle, topNCount, sortItems, assignments);
  }

  @Override
  public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(
      ConnectorSession session, ConnectorTableHandle handle, Constraint constraint) {

    MilvusTableHandle tableHandle = (MilvusTableHandle) handle;
    TupleDomain<ColumnHandle> oldDomain = tableHandle.getConstraint();

    TupleDomain<ColumnHandle> newDomain = oldDomain.intersect(constraint.getSummary());

    if (newDomain.isNone()) {
      return Optional.empty();
    }

    if (oldDomain.equals(newDomain)) {
      return Optional.empty();
    }

    handle =
        new MilvusTableHandle(tableHandle.getSchemaName(), tableHandle.getTableName(), newDomain);

    // setting remainingFilter to TupleDomain.all() because milvus is doing the actual filtering.

    return Optional.of(new ConstraintApplicationResult<>(handle, TupleDomain.all(), false));
  }
}
