package io.confluent.kafka.connect.cdc;

import org.apache.kafka.connect.data.Schema;

import java.util.List;

class SchemaAndFields {
  public final Schema schema;
  public final List<String> fields;

  public SchemaAndFields(Schema schema, List<String> fields) {
    this.schema = schema;
    this.fields = fields;
  }
}
