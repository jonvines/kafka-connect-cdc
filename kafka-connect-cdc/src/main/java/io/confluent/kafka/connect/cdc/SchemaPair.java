package io.confluent.kafka.connect.cdc;

import java.util.Map;

class SchemaPair implements Map.Entry<SchemaAndFields, SchemaAndFields> {
  private final SchemaAndFields key;
  private final SchemaAndFields value;

  public SchemaPair(SchemaAndFields key, SchemaAndFields value) {
    this.key = key;
    this.value = value;
  }

  @Override
  public SchemaAndFields getKey() {
    return this.key;
  }

  @Override
  public SchemaAndFields getValue() {
    return this.value;
  }

  @Override
  public SchemaAndFields setValue(SchemaAndFields value) {
    throw new UnsupportedOperationException("setValue is not supported.");
  }
}
