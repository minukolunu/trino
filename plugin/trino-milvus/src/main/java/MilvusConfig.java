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
import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.configuration.Config;

import javax.validation.constraints.NotNull;

public class MilvusConfig {
  private String host;

  private String port;

  @NotNull
  @JsonProperty
  public String getPort() {
    return port;
  }

  @NotNull
  @JsonProperty
  public String getHost() {
    return host;
  }

  @Config("host")
  public MilvusConfig setHost(String host) {
    this.host = host;
    return this;
  }

  @Config("port")
  public MilvusConfig setPort(String port) {
    this.port = port;
    return this;
  }
}
