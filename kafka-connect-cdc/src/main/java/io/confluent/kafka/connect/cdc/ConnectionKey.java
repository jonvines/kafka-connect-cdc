package io.confluent.kafka.connect.cdc;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Ordering;

public class ConnectionKey implements Comparable<ConnectionKey> {
  public final String server;
  public final int port;
  public final String username;
  public final String databaseName;

  public ConnectionKey(String server, int port, String username, String databaseName) {
    Preconditions.checkNotNull(server, "server cannot be null.");
    Preconditions.checkNotNull(username, "username cannot be null.");
    this.server = server;
    this.port = port;
    this.username = username;
    this.databaseName = databaseName;
  }

  public static ConnectionKey of(String serverName, int port, String username, String databaseName) {
    return new ConnectionKey(serverName, port, username, databaseName);
  }

  @Override
  public int compareTo(ConnectionKey that) {
    return ComparisonChain.start()
        .compare(this.server, that.server)
        .compare(this.port, that.port)
        .compare(this.username, that.username)
        .compare(this.databaseName, that.databaseName, Ordering.natural().nullsLast())
        .result();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(ConnectionKey.class)
        .omitNullValues()
        .add("server", this.server)
        .add("port", this.port)
        .add("username", this.username)
        .add("databaseName", this.databaseName)
        .toString();
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(
        this.server,
        this.username,
        this.databaseName
    );
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof ConnectionKey) {
      ConnectionKey that = (ConnectionKey) obj;
      return 0 == this.compareTo(that);
    } else {
      return false;
    }
  }
}
