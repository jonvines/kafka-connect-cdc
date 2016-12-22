package io.confluent.kafka.connect.cdc;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

class Constants {
  public static final String METADATA_FIELD = "_cdc_metadata";
  public static final String DATABASE_NAME_VARIABLE = "databaseName";
  public static final String SCHEMA_NAME_VARIABLE = "schemaName";
  public static final String TABLE_NAME_VARIABLE = "tableName";
  public static final String NAMESPACE_VARIABLE = "namespace";

  public static final String DATABASE_NAME_TEMPLATE_VARIABLE = "${databaseName}";
  public static final String SCHEMA_NAME_TEMPLATE_VARIABLE = "${schemaName}";
  public static final String TABLE_NAME_TEMPLATE_VARIABLE = "${tableName}";
  public static final String NAMESPACE_TEMPLATE_VARIABLE = "${namespace}";

  public static final String[] TEMPLATE_VARIABLES = new String[]{
      DATABASE_NAME_TEMPLATE_VARIABLE,
      SCHEMA_NAME_TEMPLATE_VARIABLE,
      TABLE_NAME_TEMPLATE_VARIABLE,
      NAMESPACE_TEMPLATE_VARIABLE
  };
}
