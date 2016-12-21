package io.confluent.kafka.connect.cdc;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import org.apache.kafka.connect.data.Schema;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY, getterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class JsonColumnValue implements Change.ColumnValue {
  @JsonProperty
  String columnName;
  @JsonProperty
  Object value;
  @JsonProperty
  Schema schema;

  public JsonColumnValue() {

  }

  public JsonColumnValue(String columnName, Schema schema, Object value) {
    this.columnName = columnName;
    this.schema = schema;
    this.value = value;
  }

  public void schema(Schema schema) {
    Preconditions.checkNotNull(schema, "schema cannot be null");
    this.schema = schema;
  }

  @Override
  public String columnName() {
    return this.columnName;
  }

  @Override
  public Schema schema() {
    return this.schema;
  }

  @Override
  public Object value() {
    return this.value;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this.getClass())
        .add("columnName", this.columnName)
        .add("value", this.value)
        .add("schema", this.schema)
        .omitNullValues()
        .toString();
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof Change.ColumnValue)) {
      return false;
    }

    Change.ColumnValue that = (Change.ColumnValue) obj;

    if (!this.columnName().equals(that.columnName())) {
      return false;
    }

    JsonConnectSchema otherSchema = new JsonConnectSchema(that.schema());
    if (!this.schema.equals(otherSchema)) {
      return false;
    }

    if (!this.value().equals(that.value())) {
      return false;
    }

    return true;
  }

  public static JsonColumnValue convert(Change.ColumnValue columnValue) {
    JsonColumnValue jsonColumnValue = new JsonColumnValue();
    jsonColumnValue.columnName = columnValue.columnName();
    jsonColumnValue.schema = columnValue.schema();
    jsonColumnValue.value = columnValue.value();
    return jsonColumnValue;
  }
}
