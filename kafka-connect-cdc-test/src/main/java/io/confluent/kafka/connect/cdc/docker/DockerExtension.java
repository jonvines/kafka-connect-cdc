package io.confluent.kafka.connect.cdc.docker;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.palantir.docker.compose.DockerComposeRule;
import com.palantir.docker.compose.connection.Container;
import com.palantir.docker.compose.connection.DockerPort;
import com.palantir.docker.compose.connection.waiting.ClusterHealthCheck;
import com.palantir.docker.compose.connection.waiting.ClusterWait;
import org.joda.time.Duration;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ContainerExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.reflect.Field;
import java.util.Map;

class DockerExtension implements BeforeAllCallback, AfterAllCallback, ParameterResolver {
  static final Logger log = LoggerFactory.getLogger(DockerExtension.class);

  DockerCompose findDockerComposeAnnotation(ExtensionContext extensionContext) {
    Class<?> testClass = extensionContext.getTestClass().get();

    if (log.isTraceEnabled()) {
      log.trace("Looking for DockerCompose extension on {}", testClass.getName());
    }

    DockerCompose dockerCompose = testClass.getAnnotation(DockerCompose.class);
    Preconditions.checkNotNull(dockerCompose, "DockerCompose annotation not found on %s", testClass.getName());
    Preconditions.checkState(!Strings.isNullOrEmpty(dockerCompose.dockerComposePath()), "dockerCompose.dockerComposePath() cannot be null or empty.");
    Preconditions.checkState(!Strings.isNullOrEmpty(dockerCompose.logPath()), "dockerCompose.logPath() cannot be null or empty.");
    return dockerCompose;
  }

  ExtensionContext.Namespace namespace(ExtensionContext extensionContext) {
    Class<?> testClass = extensionContext.getTestClass().get();
    ExtensionContext.Namespace namespace = ExtensionContext.Namespace.create(testClass.getName(), "docker", "compose");
    if (log.isTraceEnabled()) {
      log.trace("Created namespace {}", namespace);
    }
    return namespace;
  }

  static final String STORE_SLOT_RULE = "rule";

  @Override
  public void beforeAll(ContainerExtensionContext containerExtensionContext) throws Exception {
    Class<?> testClass = containerExtensionContext.getTestClass().get();
    ExtensionContext.Namespace namespace = namespace(containerExtensionContext);
    DockerCompose dockerCompose = findDockerComposeAnnotation(containerExtensionContext);
    ExtensionContext.Store store = containerExtensionContext.getStore(namespace);

    DockerComposeRule.Builder builder = DockerComposeRule.builder();
    builder.file(dockerCompose.dockerComposePath());

    File logPathRoot = new File(dockerCompose.logPath());
    File testClassLogPath = new File(logPathRoot, testClass.getName());

    if (log.isTraceEnabled()) {
      log.trace("Setting log path for docker compose to {}", testClassLogPath.getAbsolutePath());
    }
    builder.saveLogsTo(testClassLogPath.getAbsolutePath());


    ClusterHealthCheck clusterHealthCheck = dockerCompose.clusterHealthCheck().newInstance();
    ClusterWait clusterWait = new ClusterWait(clusterHealthCheck, Duration.standardSeconds(dockerCompose.clusterHealthCheckTimeout()));
    builder.addClusterWait(clusterWait);

    DockerComposeRule dockerComposeRule = builder.build();
    store.put(STORE_SLOT_RULE, dockerComposeRule);
    dockerComposeRule.before();
  }

  @Override
  public void afterAll(ContainerExtensionContext containerExtensionContext) throws Exception {
    ExtensionContext.Namespace namespace = namespace(containerExtensionContext);
    DockerCompose dockerCompose = findDockerComposeAnnotation(containerExtensionContext);
    ExtensionContext.Store store = containerExtensionContext.getStore(namespace);
    DockerComposeRule dockerComposeRule = store.get("rule", DockerComposeRule.class);
    dockerComposeRule.after();
  }

  @Override
  public boolean supports(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
    return
        parameterContext.getParameter().isAnnotationPresent(DockerFormatString.class)||
        parameterContext.getParameter().isAnnotationPresent(DockerContainer.class)
        ;
  }

  @Override
  public Object resolve(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
    ExtensionContext.Namespace namespace = namespace(extensionContext);
    ExtensionContext.Store store = extensionContext.getStore(namespace);
    Object result = null;
    DockerComposeRule docker = store.get(STORE_SLOT_RULE, DockerComposeRule.class);

    DockerFormatString dockerFormatString = parameterContext.getParameter().getAnnotation(DockerFormatString.class);
    DockerContainer dockerContainer = parameterContext.getParameter().getAnnotation(DockerContainer.class);
    if (null != dockerFormatString) {
      Container container = docker.containers().container(dockerFormatString.container());
      DockerPort dockerPort = container.port(dockerFormatString.port());
      result = dockerPort.inFormat(dockerFormatString.format());
    } else if(null != dockerContainer) {
      Container container = docker.containers().container(dockerContainer.container());
      result = container;
    }

    return result;
  }
}
