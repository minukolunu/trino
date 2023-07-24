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
import io.airlift.slice.Slice;
import io.milvus.response.QueryResultsWrapper;
import io.milvus.response.SearchResultsWrapper;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.type.Type;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static io.airlift.slice.Slices.utf8Slice;

public class MilvusRecordCursor implements RecordCursor {
  private QueryResultsWrapper.RowRecord currentRow;
  private Iterator<QueryResultsWrapper.RowRecord> recordIterator;
  private final Map<String, MilvusColumnHandle> handleMap;
  private final Map<Integer, String> colIdxToName;

  public MilvusRecordCursor(
      SearchResultsWrapper wrapper,
      Map<String, MilvusColumnHandle> handleMap,
      Map<Integer, String> colIdxToName) {
    List<QueryResultsWrapper.RowRecord> list = wrapper.getRowRecords();
    list.sort(Comparator.comparingDouble(o -> (Float) o.get("distance")));
    this.recordIterator = list.iterator();
    this.handleMap = handleMap;
    this.colIdxToName = colIdxToName;
  }

  @Override
  public long getCompletedBytes() {
    return 0;
  }

  @Override
  public long getReadTimeNanos() {
    return 0;
  }

  @Override
  public Type getType(int field) {
    return handleMap.get(colIdxToName.get(field)).getColumnType();
  }

  @Override
  public boolean advanceNextPosition() {
    if (recordIterator.hasNext()) {
      currentRow = recordIterator.next();
      return true;
    }
    return false;
  }

  @Override
  public boolean getBoolean(int field) {
    return (boolean) currentRow.get("product_id");
  }

  @Override
  public long getLong(int field) {
    return (long) currentRow.get("product_id");
  }

  @Override
  public double getDouble(int field) {
    return (double) currentRow.get("product_id");
  }

  @Override
  public Slice getSlice(int field) {
    return utf8Slice((String) currentRow.get("product_id"));
  }

  @Override
  public Object getObject(int field) {
    return currentRow.get("product_id");
  }

  @Override
  public boolean isNull(int field) {
    return currentRow.get("product_id") == null;
  }

  @Override
  public void close() {}
}
