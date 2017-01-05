package io.confluent.kafka.connect.cdc;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Ordering;

import java.util.Map;

public class ChangeKey implements Comparable<ChangeKey> {
  public final String databaseName;
  public final String schemaName;
  public final String tableName;


  public ChangeKey(Change change) {
    this(change.databaseName(), change.schemaName(), change.tableName());
  }

  public ChangeKey(String databaseName, String schemaName, String tableName) {
    this.databaseName = databaseName;
    this.schemaName = schemaName;
    this.tableName = tableName;
  }


  @Override
  public int compareTo(ChangeKey that) {
    return ComparisonChain.start()
        .compare(this.databaseName, that.databaseName, Ordering.natural().nullsLast())
        .compare(this.schemaName, that.schemaName, Ordering.natural().nullsLast())
        .compare(this.tableName, that.tableName, Ordering.natural().nullsLast())
        .result();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(ChangeKey.class)
        .omitNullValues()
        .add("databaseName", this.databaseName)
        .add("schemaName", this.schemaName)
        .add("tableName", this.tableName)
        .toString();
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(
        this.databaseName,
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

  public Map<String, Object> sourcePartition() {
    return Change.sourcePartition(this.databaseName, this.schemaName, this.tableName);
  }
}
