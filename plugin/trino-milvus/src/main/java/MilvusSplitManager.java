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
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedSplitSource;
import io.trino.spi.connector.TableNotFoundException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class MilvusSplitManager implements ConnectorSplitManager {

  private final MilvusClient milvusClient;
  private final MilvusConfig milvusConfig;

  @Inject
  public MilvusSplitManager(MilvusClient milvusClient, MilvusConfig milvusConfig) {
    this.milvusClient = milvusClient;
    this.milvusConfig = milvusConfig;
  }

  @Override
  public ConnectorSplitSource getSplits(
      ConnectorTransactionHandle transaction,
      ConnectorSession session,
      ConnectorTableHandle connectorTableHandle,
      DynamicFilter dynamicFilter,
      Constraint constraint) {
    MilvusTableHandle tableHandle = (MilvusTableHandle) connectorTableHandle;
    MilvusTable table =
        milvusClient.getTable(tableHandle.getSchemaName(), tableHandle.getTableName());

    // this can happen if table is removed during a query
    if (table == null) {
      throw new TableNotFoundException(tableHandle.toSchemaTableName());
    }

    List<ConnectorSplit> splits = new ArrayList<>();
    splits.add(new MilvusSplit(milvusConfig));
    Collections.shuffle(splits);

    return new FixedSplitSource(splits);
  }
}
