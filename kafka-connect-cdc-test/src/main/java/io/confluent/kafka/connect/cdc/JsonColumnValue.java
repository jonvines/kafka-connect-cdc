package io.confluent.kafka.connect.cdc;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.io.BaseEncoding;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;

import java.math.BigDecimal;

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
    Preconditions.checkNotNull(schema, "schema cannot be null");
    this.columnName = columnName;
    this.schema = schema;
    this.value = value;
  }

  static Object int8(Object o) {
    if (o instanceof Long) {
      Long integer = (Long) o;
      return integer.byteValue();
    }
    if (o instanceof Integer) {
      Integer integer = (Integer) o;
      return integer.byteValue();
    }
    return o;
  }

  static Object int16(Object o) {
    if (o instanceof Long) {
      Long integer = (Long) o;
      return integer.shortValue();
    }
    if (o instanceof Integer) {
      Integer integer = (Integer) o;
      return integer.shortValue();
    }
    return o;
  }

  static Object int32(Object o) {
    if (o instanceof Long) {
      Long integer = (Long) o;
      return integer.intValue();
    }

    return o;
  }

  static Object int64(Object o) {
    if (o instanceof Integer) {
      Integer integer = (Integer) o;
      return integer.longValue();
    }

    return o;
  }

  static Object float64(Object o) {
    if (o instanceof BigDecimal) {
      BigDecimal integer = (BigDecimal) o;
      return integer.doubleValue();
    }

    return o;
  }

  static Object float32(Object o) {
    if (o instanceof BigDecimal) {
      BigDecimal integer = (BigDecimal) o;
      return integer.floatValue();
    }

    return o;
  }

  static Object bytes(Object value) {
    if (value instanceof String) {
      String s = (String) value;
      return BaseEncoding.base64().decode(s);
    }
    return value;
  }

  static Object decimal(Schema schema, Object value) {
    if (value instanceof byte[]) {
      byte[] bytes = (byte[]) value;
      return Decimal.toLogical(schema, bytes);
    }
    return value;
  }

  static Object date(Schema schema, Object value) {
    if (value instanceof Integer) {
      Integer intValue = (Integer) value;
      return Date.toLogical(schema, intValue.intValue());
    }
    return value;
  }

  static Object time(Schema schema, Object value) {
    if (value instanceof Integer) {
      Integer intValue = (Integer) value;
      return Time.toLogical(schema, intValue.intValue());
    }
    return value;
  }

  static Object timestamp(Schema schema, Object value) {
    if (value instanceof Long) {
      Long longValue = (Long) value;
      return Timestamp.toLogical(schema, longValue.longValue());
    }
    if (value instanceof Integer) {
      Integer integerValue = (Integer) value;
      return Timestamp.toLogical(schema, integerValue.longValue());
    }
    return value;
  }

  public static JsonColumnValue convert(Change.ColumnValue columnValue) {
    JsonColumnValue jsonColumnValue = new JsonColumnValue();
    jsonColumnValue.columnName = columnValue.columnName();
    jsonColumnValue.schema = columnValue.schema();
    jsonColumnValue.value(columnValue.value());
    return jsonColumnValue;
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
    if (null == this.value) {
      return null;
    }

    Object result;

    switch (this.schema.type()) {
      case BYTES:
        if (Decimal.LOGICAL_NAME == this.schema.name()) {
          result = decimal(this.schema, this.value);
        } else {
          result = bytes(this.value);
        }
        break;
      case INT32:
        if (Date.LOGICAL_NAME.equals(this.schema.name())) {
          result = date(this.schema, this.value);
        } else if (Time.LOGICAL_NAME.equals(this.schema.name())) {
          result = time(this.schema, this.value);
        } else {
          result = int32(this.value);
        }
        break;
      case INT16:
        result = int16(this.value);
        break;
      case INT64:
        if (Timestamp.LOGICAL_NAME.equals(this.schema.name())) {
          result = timestamp(this.schema, this.value);
        } else {
          result = int64(this.value);
        }
        break;
      case INT8:
        result = int8(this.value);
        break;
      case FLOAT32:
        result = float32(this.value);
        break;
      case FLOAT64:
        result = float64(this.value);
        break;
      default:
        result = this.value;
        break;
    }

    return result;
  }

  public void value(Object value) {
    if (value instanceof java.sql.Date) {
      java.sql.Date d = (java.sql.Date) value;
      this.value = new java.util.Date(d.getTime());
    }
    this.value = value;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this.getClass())
        .add("columnName", this.columnName)
        .add("schema", this.schema)
        .add("value", this.value)
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
}
