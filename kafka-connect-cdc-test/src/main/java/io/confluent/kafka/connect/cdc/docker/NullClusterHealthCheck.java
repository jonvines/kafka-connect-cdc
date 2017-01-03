package io.confluent.kafka.connect.cdc.docker;

import com.palantir.docker.compose.connection.Cluster;
import com.palantir.docker.compose.connection.waiting.ClusterHealthCheck;
import com.palantir.docker.compose.connection.waiting.SuccessOrFailure;

import java.util.Optional;

class NullClusterHealthCheck implements ClusterHealthCheck {
  @Override
  public SuccessOrFailure isClusterHealthy(Cluster cluster) {
    return new SuccessOrFailure() {
      @Override
      protected Optional<String> optionalFailureMessage() {
        return null;
      }
    };
  }
}
