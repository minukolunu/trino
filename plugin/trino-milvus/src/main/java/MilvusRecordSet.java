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
import io.milvus.response.SearchResultsWrapper;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.RecordSet;
import io.trino.spi.type.Type;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class MilvusRecordSet implements RecordSet {

  private final SearchResultsWrapper wrapper;
  private final List<Type> types;
  private final Map<Integer, String> colIdxToName;

  private Map<String, MilvusColumnHandle> handleMap = new HashMap<>();

  public MilvusRecordSet(
      SearchResultsWrapper wrapper,
      List<? extends ColumnHandle> columns,
      Map<Integer, String> colIdxToName) {
    this.wrapper = wrapper;
    this.types =
        columns.stream()
            .map(
                column -> {
                  MilvusColumnHandle columnHandle = (MilvusColumnHandle) column;
                  if (columnHandle.getExtraInfo().contains("Vector")) {
                    return null;
                  }
                  handleMap.put(columnHandle.getColumnName(), columnHandle);
                  return columnHandle.getColumnType();
                })
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
    this.colIdxToName = colIdxToName;
  }

  @Override
  public List<Type> getColumnTypes() {
    return types;
  }

  @Override
  public RecordCursor cursor() {
    return new MilvusRecordCursor(wrapper, handleMap, colIdxToName);
  }
}
