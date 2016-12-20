package io.confluent.kafka.connect.cdc.docker;

import com.google.common.collect.ImmutableMap;
import com.palantir.docker.compose.DockerComposeRule;
import com.palantir.docker.compose.connection.Container;
import com.palantir.docker.compose.connection.waiting.HealthCheck;

import java.util.Map;

public class DockerUtils {

  static DockerComposeRule loadRule(String composeFile, String logsPath, Map<String, HealthCheck<Container>> healthChecks) {
    DockerComposeRule.Builder builder = DockerComposeRule.builder()
        .file(composeFile)
        .saveLogsTo(logsPath);

    for (Map.Entry<String, HealthCheck<Container>> kvp : healthChecks.entrySet()) {
      builder.waitingForService(kvp.getKey(), kvp.getValue());
    }

    return builder.build();
  }

  public static DockerComposeRule loadRule(String composeFile, String logsPath, String container1, HealthCheck<Container> healthCheck1) {
    return loadRule(composeFile, logsPath, ImmutableMap.of(container1, healthCheck1));
  }

  public static DockerComposeRule loadRule(String composeFile, String logsPath,
                                           String container1, HealthCheck<Container> healthCheck1,
                                           String container2, HealthCheck<Container> healthCheck2) {
    return loadRule(composeFile, logsPath, ImmutableMap.of(container1, healthCheck1, container2, healthCheck2));
  }

  public static DockerComposeRule loadRule(String composeFile, String logsPath,
                                           String container1, HealthCheck<Container> healthCheck1,
                                           String container2, HealthCheck<Container> healthCheck2,
                                           String container3, HealthCheck<Container> healthCheck3) {
    return loadRule(composeFile, logsPath, ImmutableMap.of(container1, healthCheck1, container2, healthCheck2, container3, healthCheck3));
  }
}
