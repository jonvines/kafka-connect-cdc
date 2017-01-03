package io.confluent.kafka.connect.cdc.docker;

import com.palantir.docker.compose.connection.waiting.ClusterHealthCheck;
import com.palantir.docker.compose.connection.waiting.HealthCheck;
import org.junit.jupiter.api.extension.ExtendWith;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@ExtendWith(DockerExtension.class)
public @interface DockerCompose {
  /**
   * Location of the docker-compose.yml file to use.
   * @return
   */
  String dockerComposePath();

  /**
   * Root location to output the logs to.
   * @return
   */
  String logPath() default "target/docker-compose";

  /**
   * Health check class used to check the health of the cluster.
   * @return
   */
  Class<? extends ClusterHealthCheck> clusterHealthCheck() default NullClusterHealthCheck.class;

  /**
   *
   * @return
   */
  int clusterHealthCheckTimeout() default 120;
}
