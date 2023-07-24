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
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.ColumnMetadata;
import java.net.URI;
import java.util.List;
import java.util.Optional;

public class MilvusTable {
  private final String name;
  private final List<MilvusColumn> columns;
  private final List<ColumnMetadata> columnsMetadata;

  @JsonCreator
  public MilvusTable(
      @JsonProperty("name") String name, @JsonProperty("columns") List<MilvusColumn> columns) {
    checkArgument(!isNullOrEmpty(name), "name is null or is empty");
    this.name = requireNonNull(name, "name is null");
    this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));

    ImmutableList.Builder<ColumnMetadata> columnsMetadata = ImmutableList.builder();
    for (MilvusColumn column : this.columns) {

      columnsMetadata.add(
          ColumnMetadata.builder()
              .setName(column.getName())
              .setType(column.getType())
              .setExtraInfo(Optional.of(column.getExtraInfo()))
              .build());
    }
    this.columnsMetadata = columnsMetadata.build();
  }

  @JsonProperty
  public String getName() {
    return name;
  }

  @JsonProperty
  public List<MilvusColumn> getColumns() {
    return columns;
  }

  public List<ColumnMetadata> getColumnsMetadata() {
    return columnsMetadata;
  }
}
