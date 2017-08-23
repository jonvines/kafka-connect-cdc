/**
 * Copyright © 2017 Jeremy Custenborder (jcustenborder@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.jcustenborder.kafka.connect.cdc.docker;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.palantir.docker.compose.DockerComposeRule;
import com.palantir.docker.compose.connection.Container;
import com.palantir.docker.compose.connection.DockerPort;
import com.palantir.docker.compose.connection.waiting.ClusterHealthCheck;
import com.palantir.docker.compose.connection.waiting.ClusterWait;
import org.joda.time.Duration;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.annotation.Annotation;

public class DockerExtension implements BeforeAllCallback, AfterAllCallback, ParameterResolver {
  public static final String STORE_SLOT_RULE = "rule";
  private static final Logger log = LoggerFactory.getLogger(DockerExtension.class);

  public static DockerCompose findDockerComposeAnnotation(ExtensionContext extensionContext) {
    Class<?> testClass = extensionContext.getTestClass().get();

    log.trace("Looking for DockerCompose extension on {}", testClass.getName());

    DockerCompose dockerCompose = testClass.getAnnotation(DockerCompose.class);
    Preconditions.checkNotNull(dockerCompose, "DockerCompose annotation not found on %s", testClass.getName());
    Preconditions.checkState(!Strings.isNullOrEmpty(dockerCompose.dockerComposePath()), "dockerCompose.dockerComposePath() cannot be null or empty.");
    Preconditions.checkState(!Strings.isNullOrEmpty(dockerCompose.logPath()), "dockerCompose.logPath() cannot be null or empty.");
    return dockerCompose;
  }

  public static ExtensionContext.Namespace namespace(ExtensionContext extensionContext) {
    Class<?> testClass = extensionContext.getTestClass().get();
    ExtensionContext.Namespace namespace = ExtensionContext.Namespace.create(testClass.getName(), "docker", "compose");
    log.trace("Created namespace {}", namespace);
    return namespace;
  }

  @Override
  public void beforeAll(ExtensionContext containerExtensionContext) throws Exception {
    Class<?> testClass = containerExtensionContext.getTestClass().get();
    ExtensionContext.Namespace namespace = namespace(containerExtensionContext);
    DockerCompose dockerCompose = findDockerComposeAnnotation(containerExtensionContext);
    ExtensionContext.Store store = containerExtensionContext.getStore(namespace);

    DockerComposeRule.Builder builder = DockerComposeRule.builder();
    builder.file(dockerCompose.dockerComposePath());

    File logPathRoot = new File(dockerCompose.logPath());
    File testClassLogPath = new File(logPathRoot, testClass.getName());

    log.trace("Setting log path for docker compose to {}", testClassLogPath.getAbsolutePath());
    builder.saveLogsTo(testClassLogPath.getAbsolutePath());


    ClusterHealthCheck clusterHealthCheck = dockerCompose.clusterHealthCheck().newInstance();
    ClusterWait clusterWait = new ClusterWait(clusterHealthCheck, Duration.standardSeconds(dockerCompose.clusterHealthCheckTimeout()));
    builder.addClusterWait(clusterWait);

    DockerComposeRule dockerComposeRule = builder.build();
    store.put(STORE_SLOT_RULE, dockerComposeRule);
    dockerComposeRule.before();
  }

  @Override
  public void afterAll(ExtensionContext containerExtensionContext) throws Exception {
    ExtensionContext.Namespace namespace = namespace(containerExtensionContext);
    ExtensionContext.Store store = containerExtensionContext.getStore(namespace);
    DockerComposeRule dockerComposeRule = store.get(STORE_SLOT_RULE, DockerComposeRule.class);
    dockerComposeRule.after();
  }

  private Object dockerHost(DockerComposeRule docker, Annotation annotation) {
    DockerHost dockerHost = (DockerHost) annotation;
    Container container = docker.containers().container(dockerHost.container());
    DockerPort dockerPort = container.port(dockerHost.port());
    return dockerPort.getIp();
  }

  private Object dockerPort(DockerComposeRule docker, Annotation annotation) {
    com.github.jcustenborder.kafka.connect.cdc.docker.DockerPort dockerFormatString = (com.github.jcustenborder.kafka.connect.cdc.docker.DockerPort) annotation;
    Container container = docker.containers().container(dockerFormatString.container());
    DockerPort dockerPort = container.port(dockerFormatString.port());
    return dockerPort.getExternalPort();
  }

  private Object dockerContainer(DockerComposeRule docker, Annotation annotation) {
    DockerContainer dockerContainer = (DockerContainer) annotation;
    Container container = docker.containers().container(dockerContainer.container());
    return container;
  }

  private Object dockerFormatString(DockerComposeRule docker, Annotation annotation) {
    DockerFormatString dockerFormatString = (DockerFormatString) annotation;
    Container container = docker.containers().container(dockerFormatString.container());
    DockerPort dockerPort = container.port(dockerFormatString.port());
    return dockerPort.inFormat(dockerFormatString.format());
  }

  @Override
  public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
    return
        parameterContext.getParameter().isAnnotationPresent(DockerFormatString.class) ||
            parameterContext.getParameter().isAnnotationPresent(DockerContainer.class) ||
            parameterContext.getParameter().isAnnotationPresent(com.github.jcustenborder.kafka.connect.cdc.docker.DockerPort.class) ||
            parameterContext.getParameter().isAnnotationPresent(com.github.jcustenborder.kafka.connect.cdc.docker.DockerHost.class);
  }

  @Override
  public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
    ExtensionContext.Namespace namespace = namespace(extensionContext);
    ExtensionContext.Store store = extensionContext.getStore(namespace);
    Object result = null;
    DockerComposeRule docker = store.get(STORE_SLOT_RULE, DockerComposeRule.class);

    Annotation[] annotations = parameterContext.getParameter().getAnnotations();

    log.trace("Found {} annotations for {}", annotations.length, parameterContext.getParameter().getName());

    for (Annotation annotation : annotations) {
      if (annotation instanceof DockerFormatString) {
        result = dockerFormatString(docker, annotation);
        break;
      } else if (annotation instanceof DockerContainer) {
        result = dockerContainer(docker, annotation);
        break;
      } else if (annotation instanceof com.github.jcustenborder.kafka.connect.cdc.docker.DockerPort) {
        result = dockerPort(docker, annotation);
        break;
      } else if (annotation instanceof DockerHost) {
        result = dockerHost(docker, annotation);
        break;
      }
    }
    return result;
  }
}
