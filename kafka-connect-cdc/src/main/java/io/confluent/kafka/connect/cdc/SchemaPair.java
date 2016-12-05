package io.confluent.kafka.connect.cdc;

import org.apache.kafka.connect.data.Schema;

import java.util.Map;

class SchemaPair implements Map.Entry<Schema, Schema> {
  private final Schema key;
  private final Schema value;

  public SchemaPair(Schema key, Schema value) {
    this.key = key;
    this.value = value;
  }

  @Override
  public Schema getKey() {
    return this.key;
  }

  @Override
  public Schema getValue() {
    return this.value;
  }

  @Override
  public Schema setValue(Schema value) {
    throw new UnsupportedOperationException("setValue is not supported.");
  }
}
