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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import io.airlift.json.ObjectMapperProvider;
import io.milvus.client.MilvusServiceClient;
import io.milvus.grpc.DataType;
import io.milvus.grpc.SearchResults;
import io.milvus.param.ConnectParam;
import io.milvus.param.IndexType;
import io.milvus.param.MetricType;
import io.milvus.param.collection.CreateCollectionParam;
import io.milvus.param.collection.CreateDatabaseParam;
import io.milvus.param.collection.DropCollectionParam;
import io.milvus.param.collection.DropDatabaseParam;
import io.milvus.param.collection.FieldType;
import io.milvus.param.collection.LoadCollectionParam;
import io.milvus.param.dml.SearchParam;
import io.milvus.param.index.CreateIndexParam;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Reader;
import java.lang.reflect.Type;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class TestClient {

  private MilvusServiceClient client;
  private static final OkHttpClient embeddingsClient = new OkHttpClient();
  private static final ObjectMapper mapper = new ObjectMapperProvider().get();
  private final String databaseName;

  public TestClient(String databaseName) {
    client =
        new MilvusServiceClient(
            ConnectParam.newBuilder()
                .withHost("localhost")
                //                .withDatabaseName(databaseName)
                .withPort(19530)
                .build());
    this.databaseName = databaseName;
  }

  public static List<List<Float>> generateEmbeddings(List<String> str) {
    try {
      String body = mapper.writeValueAsString(str);
      RequestBody rBody =
          RequestBody.create(
              body.getBytes(Charset.defaultCharset()), MediaType.parse("application/json"));
      Request request =
          new Request.Builder().url("http://localhost:4040/embeddings").post(rBody).build();

      Response response = embeddingsClient.newCall(request).execute();
      return mapper.readValue(
          response.body().string(),
          new TypeReference<List<List<Float>>>() {
            @Override
            public Type getType() {
              return super.getType();
            }
          });
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void createDataBase() {
    System.out.println(
        client.createDatabase(
            CreateDatabaseParam.newBuilder().withDatabaseName(databaseName).build()));
  }

  private void createCollection(String collectionName) {
    FieldType productIdType =
        FieldType.newBuilder()
            .withName("product_id")
            .withMaxLength(256)
            .withDataType(DataType.VarChar)
            .withPrimaryKey(true)
            .build();
    FieldType productNameType =
        FieldType.newBuilder()
            .withName("product_name")
            .withMaxLength(1024)
            .withDataType(DataType.VarChar)
            .build();
    FieldType productCategoryType =
        FieldType.newBuilder()
            .withName("product_category")
            .withMaxLength(1024)
            .withDataType(DataType.VarChar)
            .build();
    FieldType productReviewType =
        FieldType.newBuilder()
            .withName("product_review")
            .withMaxLength(10000)
            .withDataType(DataType.VarChar)
            .build();
    FieldType productReviewVector =
        FieldType.newBuilder()
            .withName("product_review_vector")
            .withDataType(DataType.FloatVector)
            .withDimension(384)
            .build();

    CreateCollectionParam req =
        CreateCollectionParam.newBuilder()
            .withCollectionName(collectionName)
            .withDatabaseName(databaseName)
            .withShardsNum(2)
            .addFieldType(productIdType)
            .addFieldType(productNameType)
            .addFieldType(productReviewType)
            .addFieldType(productReviewVector)
            .addFieldType(productCategoryType)
            .build();
    System.out.println(client.createCollection(req));
  }

  public void fromCsv(
      String csv,
      String fieldToVectorize,
      String collectionName,
      Integer dimension,
      String pkColumn)
      throws Exception {

    Reader in = new FileReader(csv);
    CSVParser records =
        CSVFormat.DEFAULT.builder().setHeader().setSkipHeaderRecord(true).build().parse(in);

    Map<String, Integer> headers = records.getHeaderMap();
    CreateCollectionParam.Builder req =
        CreateCollectionParam.newBuilder().withCollectionName(collectionName).withShardsNum(2);
    for (String key : headers.keySet()) {
      FieldType type =
          FieldType.newBuilder()
              .withName(key)
              .withPrimaryKey(key.equalsIgnoreCase(pkColumn))
              .withDataType(DataType.VarChar)
              .withMaxLength(5000)
              .build();
      req.addFieldType(type);
    }

    req.addFieldType(
        FieldType.newBuilder()
            .withName(fieldToVectorize + "_Vector")
            .withDataType(DataType.FloatVector)
            .withDimension(dimension)
            .build());
    CreateCollectionParam params = req.build();
    System.out.println(params);
    System.out.println(client.createCollection(params));
  }

  public void vectorizeAndSave(String collectionName)
      throws FileNotFoundException, IOException, ClassNotFoundException {
    /*List<String> productIds = new ArrayList<>();
    List<String> reviews = new ArrayList<>();
    List<String> names = new ArrayList<>();
    List<String> categories = new ArrayList<>();
    File file = new File("/Users/minukolunu/cleaned_products.csv");
    CSVReader reader = new CSVReader(new FileReader(file));
    List<String[]> data = reader.readAll();
    for (int i = 1; i < data.size(); i++) {
      String[] row = data.get(i);
      String productId = row[0];
      String review = row[row.length - 1];
      if (productId.isEmpty() || review.isEmpty()) {
        continue;
      }
      productIds.add(productId);
      names.add(row[3]);
      categories.add(row[4]);
      reviews.add(row[5]);
    }
    List<List<Float>> reviewVectors = generateEmbeddings(reviews);

    */
    /*List<List<Float>> reviewVectors = readFromFile("/Users/minukolunu/vectors.txt");*/
    /*

    List<InsertParam.Field> fields = new ArrayList<>();
    fields.add(new InsertParam.Field("product_id", productIds));
    fields.add(new InsertParam.Field("product_review_vector", reviewVectors));
    fields.add(new InsertParam.Field("product_review", reviews));
    fields.add(new InsertParam.Field("product_category", categories));
    fields.add(new InsertParam.Field("product_name", names));

    InsertParam insertParam =
        InsertParam.newBuilder()
            .withCollectionName(collectionName)
            .withDatabaseName(databaseName)
            .withFields(fields)
            .build();
    System.out.println(client.insert(insertParam));*/
  }

  private void createIndex(String collectionName, String indexField) {
    final IndexType INDEX_TYPE = IndexType.IVF_FLAT; // IndexType
    final String INDEX_PARAM = "{\"nlist\":1024}"; // ExtraParam

    System.out.println(
        client.createIndex(
            CreateIndexParam.newBuilder()
                .withCollectionName(collectionName)
                .withDatabaseName(databaseName)
                .withFieldName(indexField)
                .withIndexType(INDEX_TYPE)
                .withMetricType(MetricType.L2)
                .withExtraParam(INDEX_PARAM)
                .withSyncMode(Boolean.FALSE)
                .build()));
  }

  private List<List<Float>> readFromFile(String fileName)
      throws IOException, ClassNotFoundException {
    FileInputStream fileInputStream = new FileInputStream(fileName);
    ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream);
    List<List<Double>> list = (List<List<Double>>) objectInputStream.readObject();
    objectInputStream.close();
    List<List<Float>> ret = new ArrayList<>();
    for (List<Double> l : list) {
      List<Float> r1 = new ArrayList<>();
      for (Double d : l) {
        r1.add(d.floatValue());
      }
      ret.add(r1);
    }
    return ret;
  }

  private void saveToFile(List<List<Double>> reviewVectors) throws IOException {
    FileOutputStream fileOutputStream = new FileOutputStream("/Users/minukolunu/vectors.txt");
    ObjectOutputStream objectOutputStream = new ObjectOutputStream(fileOutputStream);
    objectOutputStream.writeObject(reviewVectors);
    objectOutputStream.flush();
    objectOutputStream.close();
  }

  public void search(String collectionName, String toSearch) {

    System.out.println(
        client.loadCollection(
            LoadCollectionParam.newBuilder().withCollectionName(collectionName).build()));

    String SEARCH_PARAM = "{\"nprobe\":10}";
    List<List<Float>> searchVectors = TestClient.generateEmbeddings(Lists.newArrayList(toSearch));
    SearchParam searchParam =
        SearchParam.newBuilder()
            .withCollectionName(collectionName)
            .withOutFields(List.of("Series_Title", "Released_Year", "Genre"))
//            .withExpr("Genre like 'Action%'")
            .withVectors(searchVectors)
            .withParams(SEARCH_PARAM)
            .withTopK(10)
            .withVectorFieldName("Overview_Vector")
            .withMetricType(MetricType.L2)
            .build();

    SearchResults results = client.search(searchParam).getData();
    System.out.println(results);
  }

  public static void main(String[] args) throws Exception {
    /* String databaseName = "default", collectionName = "product_reviews";
    TestClient client = new TestClient(databaseName);
    client.dropAll(collectionName);
    client.createDataBase();
    client.createCollection(collectionName);
    client.vectorizeAndSave(collectionName);
    client.createIndex(collectionName, "product_review_vector");
    System.out.println("Done");
    */
    /*System.out.println(generateEmbeddings(Arrays.asList("asdf", "asdf1")));*/
    /*
    //    client.search(collectionName);*/

    TestClient client = new TestClient("asdf");
    //    client.fromCsv("/Users/minukolunu/imdb.csv", "Overview", "IMDB", 384, "Poster_Link");
    client.search("IMDB", "Superhero movies");
  }

  private void dropAll(String collectionName) {
    System.out.println(
        client.dropCollection(
            DropCollectionParam.newBuilder()
                .withCollectionName(collectionName)
                .withDatabaseName(databaseName)
                .build()));
    System.out.println(
        client.dropDatabase(DropDatabaseParam.newBuilder().withDatabaseName(databaseName).build()));
  }
}
