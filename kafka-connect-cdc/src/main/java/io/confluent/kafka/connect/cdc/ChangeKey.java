package io.confluent.kafka.connect.cdc;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.collect.ComparisonChain;

class ChangeKey implements Comparable<ChangeKey> {
  public final String sourceDatabaseName;
  public final String tableName;

  public ChangeKey(Change change) {
    this.sourceDatabaseName = change.sourceDatabaseName();
    this.tableName = change.tableName();
  }


  @Override
  public int compareTo(ChangeKey that) {
    return ComparisonChain.start()
        .compare(this.sourceDatabaseName, that.sourceDatabaseName)
        .compare(this.tableName, that.tableName)
        .result();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(ChangeKey.class)
        .add("sourceDatabaseName", this.sourceDatabaseName)
        .add("tableName", this.tableName)
        .toString();
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(
        this.sourceDatabaseName,
        this.tableName
    );
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof ChangeKey) {
      ChangeKey that = (ChangeKey) obj;
      return 0 == this.compareTo(that);
    } else {
      return false;
    }
  }
}
