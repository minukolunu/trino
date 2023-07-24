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

import com.google.inject.Inject;
import io.airlift.slice.Slice;
import io.milvus.response.SearchResultsWrapper;

import io.trino.spi.ErrorCode;
import io.trino.spi.ErrorType;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorRecordSetProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.RecordSet;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

public class MilvusRecordSetProvider implements ConnectorRecordSetProvider {

  private final MilvusClient milvusClient;

  @Inject
  public MilvusRecordSetProvider(MilvusClient client) {
    this.milvusClient = client;
  }

  @Override
  public RecordSet getRecordSet(
      ConnectorTransactionHandle transaction,
      ConnectorSession session,
      ConnectorSplit split,
      ConnectorTableHandle table,
      List<? extends ColumnHandle> columns) {

    MilvusTableHandle tableHandle = (MilvusTableHandle) table;

    Optional<String> like = getLike(tableHandle);
    if (like.isEmpty()) {
      throw new TrinoException(
          () -> new ErrorCode(2, "Like Absent", ErrorType.USER_ERROR),
          "Like must be present when using vector tables");
    }
    String tableName = tableHandle.getTableName();
    String databaseName = tableHandle.getSchemaName();
    milvusClient.loadCollection(databaseName, tableName);
    Map<Integer, String> colIdxToName = new HashMap<>();
    for (int i = 0; i < columns.size(); i++) {
      MilvusColumnHandle columnHandle = (MilvusColumnHandle) columns.get(i);
      if (columnHandle.getExtraInfo().contains("Vector")) {
        continue;
      }
      colIdxToName.put(i, columnHandle.getColumnName());
    }
    SearchResultsWrapper resultsWrapper =
        milvusClient.getRecords(
            databaseName,
            tableName,
            like.get(),
            columns.stream()
                .map(
                    handle -> {
                      MilvusColumnHandle columnHandle = (MilvusColumnHandle) handle;
                      if (columnHandle.getExtraInfo().equalsIgnoreCase("Vector Type")) {
                        return null;
                      }
                      return columnHandle.getColumnName();
                    })
                .filter(Objects::nonNull)
                .collect(Collectors.toList()));

    return new MilvusRecordSet(resultsWrapper, columns, colIdxToName);
  }

  private Optional<String> getLike(MilvusTableHandle handle) {
    TupleDomain<ColumnHandle> domain = handle.getConstraint();
    for (Map.Entry<ColumnHandle, Domain> entry : domain.getDomains().get().entrySet()) {
      MilvusColumnHandle columnHandle = (MilvusColumnHandle) entry.getKey();
      if (columnHandle.getExtraInfo().equalsIgnoreCase("Vector Type")) {
        // This is the stirng we need to vectorize
        Object obj = entry.getValue().getSingleValue();
        if (obj instanceof Slice slice) {
          return Optional.of(new String(slice.byteArray(), Charset.defaultCharset()));
        } else {
          throw new TrinoException(
              () -> new ErrorCode(1, "User Error", ErrorType.USER_ERROR),
              "Filter must be of type string");
        }
      }
    }
    return Optional.empty();
  }
}
