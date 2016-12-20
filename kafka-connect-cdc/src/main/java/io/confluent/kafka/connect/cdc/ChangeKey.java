package io.confluent.kafka.connect.cdc;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.collect.ComparisonChain;

class ChangeKey implements Comparable<ChangeKey> {
  public final String schemaName;
  public final String tableName;

  public ChangeKey(Change change) {
    this(change.schemaName(), change.tableName());
  }

  public ChangeKey(String schemaName, String tableName) {
    this.schemaName = schemaName;
    this.tableName = tableName;
  }


  @Override
  public int compareTo(ChangeKey that) {
    return ComparisonChain.start()
        .compare(this.schemaName, that.schemaName)
        .compare(this.tableName, that.tableName)
        .result();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(ChangeKey.class)
        .add("schemaName", this.schemaName)
        .add("tableName", this.tableName)
        .toString();
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(
        this.schemaName,
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
