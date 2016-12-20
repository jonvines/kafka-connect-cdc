package io.confluent.kafka.connect.cdc;

import org.apache.kafka.connect.data.Struct;

import java.util.Map;

class StructPair implements Map.Entry<Struct, Struct> {
  private final Struct key;
  private final Struct value;

  public StructPair(SchemaPair schemaPair) {
    this.key = new Struct(schemaPair.getKey().schema);
    this.value = new Struct(schemaPair.getValue().schema);
  }

  @Override
  public Struct getKey() {
    return this.key;
  }

  @Override
  public Struct getValue() {
    return this.value;
  }

  @Override
  public Struct setValue(Struct value) {
    throw new UnsupportedOperationException("setValue is not supported.");
  }
}
