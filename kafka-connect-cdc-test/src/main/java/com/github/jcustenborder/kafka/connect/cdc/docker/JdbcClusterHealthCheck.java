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
import com.palantir.docker.compose.connection.Cluster;
import com.palantir.docker.compose.connection.Container;
import com.palantir.docker.compose.connection.DockerPort;
import com.palantir.docker.compose.connection.State;
import com.palantir.docker.compose.connection.waiting.Attempt;
import com.palantir.docker.compose.connection.waiting.ClusterHealthCheck;
import com.palantir.docker.compose.connection.waiting.SuccessOrFailure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;

public class JdbcClusterHealthCheck implements ClusterHealthCheck {
  private static final Logger log = LoggerFactory.getLogger(JdbcClusterHealthCheck.class);

  final String containerName;
  final int port;
  final String jdbcUrlFormat;
  final String username;
  final String password;

  public JdbcClusterHealthCheck(String containerName, int port, String jdbcUrlFormat, String username, String password) {
    this.containerName = containerName;
    this.port = port;
    this.jdbcUrlFormat = jdbcUrlFormat;
    this.username = username;
    this.password = password;
  }

  @Override
  public SuccessOrFailure isClusterHealthy(Cluster cluster) {
    final Container container = cluster.container(containerName);
    try {
      Preconditions.checkState(State.Up == container.state(), "container %s should be up", containerName);
    } catch (IOException e) {
      log.error("exception thrown", e);
    } catch (InterruptedException e) {
      log.error("exception thrown", e);
    }
    return SuccessOrFailure.onResultOf(
        new Attempt() {
          @Override
          public boolean attempt() throws Exception {

            final DockerPort dockerPort = container.port(port);
            final String jdbcUrl = dockerPort.inFormat(jdbcUrlFormat);

            log.trace("Container Ip = {}", dockerPort.getIp());
            if (!dockerPort.isListeningNow()) {
              log.trace("Port {} is not listening on container {}.", port, containerName);
              return false;
            }

            log.trace("Attempting to authenticate to {} with user {}.", jdbcUrl, username);
            try (Connection connection = DriverManager.getConnection(
                jdbcUrl,
                username,
                password
            )) {
              return true;
            } catch (Exception ex) {
              log.trace("Exception thrown", ex);

              Thread.sleep(2500);
              return false;
            }
          }
        }
    );
  }
}
