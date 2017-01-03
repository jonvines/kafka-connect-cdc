package io.confluent.kafka.connect.cdc;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import com.google.common.io.BaseEncoding;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY, getterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class JsonChange implements Change {
  @JsonProperty
  Map<String, String> metadata = new LinkedHashMap<>();
  @JsonProperty
  Map<String, Object> sourcePartition = new LinkedHashMap<>();
  @JsonProperty
  Map<String, Object> sourceOffset = new LinkedHashMap<>();
  @JsonProperty
  String databaseName;
  @JsonProperty
  String schemaName;
  @JsonProperty
  String tableName;
  @JsonProperty
  ChangeType changeType;
  @JsonProperty
  long timestamp;

  @JsonDeserialize(contentAs = JsonColumnValue.class)
  @JsonProperty
  List<ColumnValue> keyColumns = new ArrayList<>();

  @JsonDeserialize(contentAs = JsonColumnValue.class)
  @JsonProperty
  List<ColumnValue> valueColumns = new ArrayList<>();

  public static void write(File file, JsonChange change) throws IOException {
    try (OutputStream outputStream = new FileOutputStream(file)) {
      ObjectMapperFactory.instance.writeValue(outputStream, change);
    }
  }

  public static void write(OutputStream outputStream, JsonChange change) throws IOException {
    ObjectMapperFactory.instance.writeValue(outputStream, change);
  }

  public static JsonChange read(InputStream inputStream) throws IOException {
    return ObjectMapperFactory.instance.readValue(inputStream, JsonChange.class);
  }

  public static JsonChange convert(Change change) {
    JsonChange jsonChange = new JsonChange();
    jsonChange.sourceOffset = change.sourceOffset();
    jsonChange.sourcePartition = change.sourcePartition();
    jsonChange.metadata = change.metadata();
    jsonChange.changeType = change.changeType();
    jsonChange.databaseName = change.databaseName();
    jsonChange.schemaName = change.schemaName();
    jsonChange.tableName = change.tableName();
    jsonChange.timestamp = change.timestamp();

    for (Change.ColumnValue columnValue : change.valueColumns()) {
      JsonColumnValue jsonColumnValue = JsonColumnValue.convert(columnValue);
      jsonChange.valueColumns().add(jsonColumnValue);
    }
    for (Change.ColumnValue columnValue : change.keyColumns()) {
      JsonColumnValue jsonColumnValue = JsonColumnValue.convert(columnValue);
      jsonChange.keyColumns().add(jsonColumnValue);
    }
    return jsonChange;
  }

  @Override
  public Map<String, String> metadata() {
    return this.metadata;
  }

  @Override
  public Map<String, Object> sourcePartition() {
    return this.sourcePartition;
  }

  @Override
  public Map<String, Object> sourceOffset() {
    return this.sourceOffset;
  }

  @Override
  public String databaseName() {
    return this.databaseName;
  }

  @Override
  public String schemaName() {
    return this.schemaName;
  }

  @Override
  public String tableName() {
    return this.tableName;
  }

  @Override
  public List<ColumnValue> keyColumns() {
    return this.keyColumns;
  }

  @Override
  public List<ColumnValue> valueColumns() {
    return this.valueColumns;
  }

  @Override
  public ChangeType changeType() {
    return this.changeType;
  }

  @Override
  public long timestamp() {
    return this.timestamp;
  }

  boolean equals(List<ColumnValue> thisList, List<ColumnValue> thatList) {
    if (thisList.size() != thatList.size()) {
      return false;
    }

    for (int i = 0; i < thisList.size(); i++) {
      ColumnValue thisColumnValue = thisList.get(i);
      ColumnValue thatColumnValue = thatList.get(i);
      if (!thisColumnValue.equals(thatColumnValue)) {
        return false;
      }
    }


    return true;
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof Change)) {
      return false;
    }

    Change that = (Change) obj;

    if (!this.changeType().equals(that.changeType()))
      return false;
    if (this.timestamp() != that.timestamp()) {
      return false;
    }
    if (!this.schemaName().equals(that.schemaName()))
      return false;
    if (!this.tableName().equals(that.tableName()))
      return false;
    MapDifference<String, String> metadataDifference = Maps.difference(this.metadata(), that.metadata());
    if (!metadataDifference.areEqual())
      return false;
    MapDifference<String, Object> sourceOffsetDifference = Maps.difference(this.sourceOffset(), that.sourceOffset());
    if (!sourceOffsetDifference.areEqual())
      return false;
    MapDifference<String, Object> sourcePartitionDifference = Maps.difference(this.sourcePartition(), that.sourcePartition());
    if (!sourcePartitionDifference.areEqual())
      return false;

    if (!equals(this.keyColumns(), that.keyColumns())) {
      return false;
    }

    if (!equals(this.valueColumns(), that.valueColumns())) {
      return false;
    }

    return true;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this.getClass())
        .add("schemaName", this.schemaName)
        .add("tableName", this.tableName)
        .add("changeType", this.changeType)
        .add("metadata", this.metadata)
        .add("sourcePartition", this.sourcePartition)
        .add("sourceOffset", this.sourceOffset)
        .add("timestamp", this.timestamp)
        .add("keyColumns", this.keyColumns)
        .add("valueColumns", this.valueColumns)

        .omitNullValues()
        .toString();
  }

  public void changeType(ChangeType value) {
    this.changeType = value;
  }

  public void tableName(String value) {
    this.tableName = value;
  }

  public void schemaName(String value) {
    this.schemaName = value;
  }

  public void timestamp(long value) {
    this.timestamp = value;
  }

  @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY, getterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
  public static class JsonColumnValue implements ColumnValue {
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
        Number number = (Number) o;
        return number.byteValue();
      }
      return o;
    }

    static Object int16(Object o) {
      if (o instanceof Long) {
        Number number = (Number) o;
        return number.shortValue();
      }
      return o;
    }

    static Object int32(Object o) {
      if (o instanceof Long) {
        Number number = (Number) o;
        return number.intValue();
      }

      return o;
    }

    static Object int64(Object value) {
      if (value instanceof Number) {
        Number number = (Number) value;
        return number.longValue();
      }
      return value;
    }

    static Object float64(Object o) {
      if (o instanceof Number) {
        Number integer = (Number) o;
        return integer.doubleValue();
      }

      return o;
    }

    static Object float32(Object o) {
      if (o instanceof Number) {
        Number integer = (Number) o;
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
      if (value instanceof BigDecimal) {
        BigDecimal decimal = (BigDecimal) value;
        final int scale = Integer.parseInt(schema.parameters().get(Decimal.SCALE_FIELD));
        if (scale == decimal.scale()) {
          return decimal;
        } else {
          return decimal.setScale(scale);
        }
      }
      if (value instanceof Number) {
        Number number = (Number) value;
        int scale = Integer.parseInt(schema.parameters().get(Decimal.SCALE_FIELD));
        BigDecimal decimal = BigDecimal.valueOf(number.longValue(), scale);
        return decimal;
      }

      return value;
    }

    static Object date(Schema schema, Object value) {
      if (value instanceof Number) {
        Number number = (Number) value;
        return Date.toLogical(schema, number.intValue());
      }
      return value;
    }

    static Object time(Schema schema, Object value) {
      if (value instanceof Number) {
        Number number = (Number) value;
        return Time.toLogical(schema, number.intValue());
      }
      return value;
    }

    static Object timestamp(Schema schema, Object value) {
      if (value instanceof Number) {
        Number number = (Number) value;
        return Timestamp.toLogical(schema, number.longValue());
      }
      return value;
    }

    public static JsonColumnValue convert(ColumnValue columnValue) {
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
          if (Decimal.LOGICAL_NAME.equals(this.schema.name())) {
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
      if (!(obj instanceof ColumnValue)) {
        return false;
      }

      ColumnValue that = (ColumnValue) obj;

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
}
