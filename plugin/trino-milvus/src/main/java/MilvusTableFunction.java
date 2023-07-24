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
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.function.table.AbstractConnectorTableFunction;
import io.trino.spi.function.table.Argument;
import io.trino.spi.function.table.ArgumentSpecification;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.function.table.Descriptor;
import io.trino.spi.function.table.ReturnTypeSpecification;
import io.trino.spi.function.table.ScalarArgument;
import io.trino.spi.function.table.TableArgument;
import io.trino.spi.function.table.TableFunctionAnalysis;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public class MilvusTableFunction extends AbstractConnectorTableFunction {
  public MilvusTableFunction(
      String schema,
      String name,
      List<ArgumentSpecification> arguments,
      ReturnTypeSpecification returnTypeSpecification) {
    super(schema, name, arguments, returnTypeSpecification);
  }

  @Override
  public TableFunctionAnalysis analyze(
      ConnectorSession session,
      ConnectorTransactionHandle transaction,
      Map<String, Argument> arguments,
      ConnectorAccessControl accessControl) {
    String collectionName =
        ((Slice) ((ScalarArgument) arguments.get("CollectionName")).getValue()).toStringUtf8();
    String searchText =
        ((Slice) ((ScalarArgument) arguments.get("SearchText")).getValue()).toStringUtf8();
    String metric = ((Slice) ((ScalarArgument) arguments.get("Metric")).getValue()).toStringUtf8();
    return TableFunctionAnalysis.builder()
        .handle(
            new ConnectorTableFunctionHandle() {
              @Override
              public int hashCode() {
                return super.hashCode();
              }
            })
        .returnedType(new Descriptor(List.of(new Descriptor.Field("Test", Optional.empty()))))
        .build();
  }
}
