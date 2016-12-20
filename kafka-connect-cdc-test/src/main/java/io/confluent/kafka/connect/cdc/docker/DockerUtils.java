package io.confluent.kafka.connect.cdc.docker;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.palantir.docker.compose.DockerComposeRule;
import com.palantir.docker.compose.connection.Container;
import com.palantir.docker.compose.connection.waiting.HealthCheck;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Map;
import java.util.Set;

public class DockerUtils {
  static final Logger log = LoggerFactory.getLogger(DockerUtils.class);

  static DockerComposeRule loadRule(String composeFile, String logsPath, Map<String, HealthCheck<Container>> healthChecks) {
    StackTraceElement[] stackTraceElements = Thread.currentThread().getStackTrace();

    Set<String> skipClasses = ImmutableSet.of(DockerUtils.class.getName(), Thread.class.getName());

    String className = null;

    for (StackTraceElement e : stackTraceElements) {
      if (skipClasses.contains(e.getClassName())) {
        continue;
      }
      className = e.getClassName();
      break;
    }

    File classLogsPath = new File(logsPath, className);

    if (log.isInfoEnabled()) {
      log.info("Setting docker logs to {}", classLogsPath);
    }

    DockerComposeRule.Builder builder = DockerComposeRule.builder()
        .file(composeFile)
        .saveLogsTo(classLogsPath.toString());

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
