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
import static io.trino.spi.function.table.ReturnTypeSpecification.GenericTable.GENERIC_TABLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

import com.google.inject.Inject;

import io.airlift.bootstrap.LifeCycleManager;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorRecordSetProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.function.table.ConnectorTableFunction;
import io.trino.spi.function.table.ScalarArgumentSpecification;
import io.trino.spi.function.table.TableArgumentSpecification;
import io.trino.spi.transaction.IsolationLevel;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Set;

public class MilvusConnector implements Connector {

  private final LifeCycleManager lifeCycleManager;
  private final MilvusConnectorMetadata metadata;
  private final MilvusSplitManager splitManager;
  private final MilvusRecordSetProvider recordSetProvider;

  @Inject
  public MilvusConnector(
      LifeCycleManager lifeCycleManager,
      MilvusConnectorMetadata metadata,
      MilvusSplitManager splitManager,
      MilvusRecordSetProvider recordSetProvider) {
    this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
    this.metadata = requireNonNull(metadata, "metadata is null");
    this.splitManager = requireNonNull(splitManager, "splitManager is null");
    this.recordSetProvider = requireNonNull(recordSetProvider, "recordSetProvider is null");
  }

  @Override
  public Set<ConnectorTableFunction> getTableFunctions() {
    return Set.of(
        new MilvusTableFunction(
            "system",
            "vector_search",
            List.of(
                ScalarArgumentSpecification.builder().type(VARCHAR).name("collection").build(),
                TableArgumentSpecification.builder()
                    .name("query")
                    .rowSemantics()
                    .pruneWhenEmpty()
                    .passThroughColumns()
                    .build()),
            GENERIC_TABLE));
  }

  @Override
  public ConnectorTransactionHandle beginTransaction(
      IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit) {
    return MilvusTransactionHandle.INSTANCE;
  }

  @Override
  public ConnectorMetadata getMetadata(
      ConnectorSession session, ConnectorTransactionHandle transactionHandle) {
    return metadata;
  }

  @Override
  public ConnectorSplitManager getSplitManager() {
    return splitManager;
  }

  @Override
  public ConnectorRecordSetProvider getRecordSetProvider() {
    return recordSetProvider;
  }

  @Override
  public final void shutdown() {
    lifeCycleManager.stop();
  }
}
