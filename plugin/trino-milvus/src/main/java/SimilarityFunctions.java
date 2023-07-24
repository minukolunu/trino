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
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;

import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SimilarityFunctions {

  private static final ObjectMapper mapper = new ObjectMapperProvider().get();

  @Inject
  public SimilarityFunctions() {}

  private float calcIp(int dimension, long[] left, long[] right, int rIndex, int lIndex) {
    float sum = 0;
    int lFrom = lIndex * dimension;
    int rFrom = rIndex * dimension;

    for (int i = 0; i < dimension; i++) {
      sum += left[lFrom + i] * right[rFrom + i];
    }

    return sum;
  }

  private float calcL2(int dimension, long[] left, long[] right, int rIndex, int lIndex) {
    float sum = 0;
    int lFrom = lIndex * dimension;
    int rFrom = rIndex * dimension;

    for (int i = 0; i < dimension; i++) {
      float gap = left[lFrom + i] - right[rFrom + i];
      sum += gap * gap;
    }

    return sum;
  }

  private static double cosineSimilarity(double[] vectorA, double[] vectorB) {
    double dotProduct = 0.0;
    double normA = 0.0;
    double normB = 0.0;
    for (int i = 0; i < vectorA.length; i++) {
      dotProduct += vectorA[i] * vectorB[i];
      normA += Math.pow(vectorA[i], 2);
      normB += Math.pow(vectorB[i], 2);
    }
    return dotProduct / (Math.sqrt(normA) * Math.sqrt(normB));
  }

  private static double eDist(double[] a, double[] b) {
    if (a.length == 0 || b.length == 0) {
      return Double.MAX_VALUE;
    }
    double diffSquareSum = 0.0;
    for (int i = 0; i < a.length; i++) {
      diffSquareSum += (a[i] - b[i]) * (a[i] - b[i]);
    }
    return diffSquareSum;
  }

  @ScalarFunction(value = "similarity")
  @Description("Returns Similarity metric for a vector")
  @SqlType(StandardTypes.DOUBLE)
  @SqlNullable
  public static Double similarity(
      @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice desc,
      @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice stringToVectorize,
      @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice metric) {
    try {
      List<Float> vectors =
          TestClient.generateEmbeddings(
                  List.of(new String(stringToVectorize.getBytes(), Charset.defaultCharset())))
              .get(0);

      List<Float> current =
          TestClient.generateEmbeddings(
                  List.of(new String(desc.getBytes(), Charset.defaultCharset())))
              .get(0);

      if (new String(metric.getBytes(), Charset.defaultCharset()).equalsIgnoreCase("cosine")) {
        return cosineSimilarity(
            vectors.stream().mapToDouble(value -> value).toArray(),
            current.stream().mapToDouble(value -> value).toArray());
      } else {
        return eDist(
            vectors.stream().mapToDouble(value -> value).toArray(),
            current.stream().mapToDouble(value -> value).toArray());
      }
    } catch (Exception e) {
      e.printStackTrace();
      return Double.MAX_VALUE;
    }
  }

  private static final Map<String, List<Float>> map = new ConcurrentHashMap<>();

  @ScalarFunction(value = "similarityScore")
  @Description("Returns Similarity metric for a vector")
  @SqlType(StandardTypes.DOUBLE)
  @SqlNullable
  public static Double similarityScore(
      @SqlNullable @SqlType(StandardTypes.VARBINARY) Slice vector,
      @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice stringToVectorize,
      @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice metric) {
    try {
      String toVectorize = new String(stringToVectorize.getBytes(), Charset.defaultCharset());
      List<Float> vectors;
      if (map.containsKey(toVectorize)) {
        vectors = map.get(toVectorize);
      } else {
        vectors = TestClient.generateEmbeddings(List.of(toVectorize)).get(0);
        map.put(toVectorize, vectors);
      }
      List<Float> current;
      try {
        current =
            mapper.readValue(
                vector.getBytes(),
                new TypeReference<List<Float>>() {
                  @Override
                  public Type getType() {
                    return super.getType();
                  }
                });
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

      if (new String(metric.getBytes(), Charset.defaultCharset()).equalsIgnoreCase("cosine")) {
        return cosineSimilarity(
            vectors.stream().mapToDouble(value -> value).toArray(),
            current.stream().mapToDouble(value -> value).toArray());
      } else {
        return eDist(
            vectors.stream().mapToDouble(value -> value).toArray(),
            current.stream().mapToDouble(value -> value).toArray());
      }
    } catch (Exception e) {
      e.printStackTrace();
      return Double.MAX_VALUE;
    }
  }

  @ScalarFunction(value = "vectorize")
  @Description("Returns Similarity metric for a vector")
  @SqlType(StandardTypes.VARCHAR)
  @SqlNullable
  public static Slice vectorize(@SqlNullable @SqlType(StandardTypes.VARCHAR) Slice desc) {
    List<Float> vectors =
        TestClient.generateEmbeddings(
                List.of(new String(desc.getBytes(), Charset.defaultCharset())))
            .get(0);
    try {
      return Slices.utf8Slice(mapper.writeValueAsString(vectors));
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  public static void main(String[] args) {}
}
