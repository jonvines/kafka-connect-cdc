package io.confluent.kafka.connect.cdc;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.common.base.Strings;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.util.Map;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY, getterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class JsonConnectSchema {
  @JsonProperty
  String name;
  @JsonProperty
  String doc;
  @JsonProperty
  Schema.Type type;
  @JsonProperty
  Object defaultValue;
  @JsonProperty
  Integer version;
  @JsonProperty
  Map<String, String> parameters;
  @JsonProperty
  boolean isOptional;

  @JsonProperty
  JsonConnectSchema keySchema;
  @JsonProperty
  JsonConnectSchema valueSchema;

  public JsonConnectSchema() {

  }

  public JsonConnectSchema(Schema schema) {
    this.name = schema.name();
    this.doc = schema.doc();
    this.type = schema.type();
    this.defaultValue = schema.defaultValue();
    this.version = schema.version();
    this.parameters = schema.parameters();
    this.isOptional = schema.isOptional();

    if (Schema.Type.MAP == this.type) {
      this.keySchema = new JsonConnectSchema(schema.keySchema());
      this.valueSchema = new JsonConnectSchema(schema.valueSchema());
    } else {
      this.keySchema = null;
      this.valueSchema = null;
    }
  }

  public Schema build() {
    SchemaBuilder builder = SchemaBuilder.type(this.type);

    if (!Strings.isNullOrEmpty(this.name)) {
      builder.name(this.name);
    }

    if (!Strings.isNullOrEmpty(this.doc)) {
      builder.doc(this.doc);
    }

    if (null != this.defaultValue) {
      builder.defaultValue(this.defaultValue);
    }

    if (null != this.parameters) {
      builder.parameters(this.parameters);
    }

    if(this.isOptional){
      builder.optional();
    }

    return builder.build();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this.getClass())
        .add("type", this.type)
        .add("name", this.name)
        .add("isOptional", this.isOptional)
        .add("version", this.version)
        .add("keySchema", this.keySchema)
        .add("valueSchema", this.valueSchema)
        .add("parameters", this.parameters)
        .add("doc", this.doc)
        .add("defaultValue", this.defaultValue)
        .omitNullValues()
        .toString();
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof JsonConnectSchema)) {
      return false;
    }

    JsonConnectSchema that = (JsonConnectSchema) obj;

    if (this.name != that.name) {
      return false;
    }
    if (this.type != that.type) {
      return false;
    }
    if (this.doc != that.doc) {
      return false;
    }
    if (this.isOptional != that.isOptional) {
      return false;
    }
    if (this.version != that.version) {
      return false;
    }
    if (this.defaultValue != that.defaultValue) {
      return false;
    }

    if (Schema.Type.MAP == this.type) {
      if (this.keySchema.equals(that.keySchema)) {
        return false;
      }
      if (this.valueSchema.equals(that.keySchema)) {
        return false;
      }
    } else if (Schema.Type.ARRAY == this.type) {
      if (this.valueSchema.equals(that.keySchema)) {
        return false;
      }
    }

    return true;
  }
}
