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
import static com.google.common.collect.Maps.transformValues;
import static com.google.common.collect.Maps.uniqueIndex;
import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import io.airlift.json.JsonCodec;
import io.milvus.client.MilvusServiceClient;
import io.milvus.grpc.DataType;
import io.milvus.grpc.DescribeCollectionResponse;
import io.milvus.grpc.FieldSchema;
import io.milvus.grpc.SearchResults;
import io.milvus.param.ConnectParam;
import io.milvus.param.MetricType;
import io.milvus.param.collection.DescribeCollectionParam;
import io.milvus.param.collection.LoadCollectionParam;
import io.milvus.param.collection.ShowCollectionsParam;
import io.milvus.param.dml.SearchParam;
import io.milvus.response.SearchResultsWrapper;
import io.trino.spi.type.*;

import java.util.*;

public class MilvusClient {

  private final MilvusServiceClient milvusServiceClient;

  private final Map<String, MilvusServiceClient> dbToMilvusClient = new HashMap<>();

  private final MilvusConfig config;

  Map<DataType, Type> typeMap =
      ImmutableMap.ofEntries(
          Map.entry(DataType.Int64, BigintType.BIGINT),
          Map.entry(DataType.Int32, IntegerType.INTEGER),
          Map.entry(DataType.String, VarcharType.VARCHAR),
          Map.entry(DataType.VarChar, VarcharType.VARCHAR),
          Map.entry(DataType.FloatVector, VarcharType.VARCHAR));

  @Inject
  public MilvusClient(MilvusConfig config, JsonCodec<Map<String, List<MilvusTable>>> catalogCodec) {
    requireNonNull(catalogCodec, "catalogCodec is null");
    this.milvusServiceClient =
        new MilvusServiceClient(
            ConnectParam.newBuilder()
                .withHost(config.getHost())
                .withPort(Integer.parseInt(config.getPort()))
                .build());
    dbToMilvusClient.put("default", milvusServiceClient);
    this.config = config;
  }

  public MilvusServiceClient getMilvusServiceClient(){
      return this.milvusServiceClient;
  }

  public Set<String> getSchemaNames() {
    return new HashSet<>(milvusServiceClient.listDatabases().getData().getDbNamesList());
  }

  public Set<String> getTableNames(String schema) {
    requireNonNull(schema, "schema is null");
    return new HashSet<>(
        milvusServiceClient
            .showCollections(ShowCollectionsParam.newBuilder().withDatabaseName(schema).build())
            .getData()
            .getCollectionNamesList());
  }

  public MilvusTable getTable(String schema, String collectionName) {

    requireNonNull(schema, "schema is null");
    requireNonNull(collectionName, "collectionName is null");
    DescribeCollectionResponse response =
        milvusServiceClient
            .describeCollection(
                DescribeCollectionParam.newBuilder()
                    .withDatabaseName(schema)
                    .withCollectionName(collectionName)
                    .build())
            .getData();
    String tableName = response.getCollectionName();
    List<MilvusColumn> milvusColumns = new ArrayList<>();
    for (FieldSchema fieldSchema : response.getSchema().getFieldsList()) {
      String fieldName = fieldSchema.getName();
      Type fieldType = typeMap.get(fieldSchema.getDataType());
      if (fieldSchema.getDataType().equals(DataType.FloatVector)) {
        milvusColumns.add(new MilvusColumn(fieldName, fieldType, "Vector Type"));
      } else {
        milvusColumns.add(new MilvusColumn(fieldName, fieldType, ""));
      }
    }
    return new MilvusTable(tableName, milvusColumns);
  }

  public void loadCollection(String databaseName, String collectionName) {

    milvusServiceClient.loadCollection(
        LoadCollectionParam.newBuilder()
            .withCollectionName(collectionName)
            .withDatabaseName(databaseName)
            .build());
  }

  public SearchResultsWrapper getRecords(
      String databaseName, String collectionName, String similarTo, List<String> columns) {
    MilvusServiceClient serviceClient = dbToMilvusClient.get(databaseName);
    if (serviceClient == null) {
      serviceClient =
          new MilvusServiceClient(
              ConnectParam.newBuilder()
                  .withHost(config.getHost())
                  .withDatabaseName(databaseName)
                  .withPort(Integer.parseInt(config.getPort()))
                  .build());
      dbToMilvusClient.put(databaseName, serviceClient);
    }
    String SEARCH_PARAM = "{\"nprobe\":10}";
    List<List<Float>> searchVectors = TestClient.generateEmbeddings(Lists.newArrayList(similarTo));
    SearchParam searchParam =
        SearchParam.newBuilder()
            .withCollectionName(collectionName)
            .withOutFields(columns)
            .withVectors(searchVectors)
            .withParams(SEARCH_PARAM)
            .withTopK(16384)
            .withVectorFieldName("product_review_vector")
            .withMetricType(MetricType.L2)
            .build();

    SearchResults results = serviceClient.search(searchParam).getData();
    SearchResultsWrapper wrapper = new SearchResultsWrapper(results.getResults());
    return wrapper;
  }
}
