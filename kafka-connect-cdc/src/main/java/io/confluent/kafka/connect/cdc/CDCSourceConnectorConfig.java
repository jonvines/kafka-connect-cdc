package io.confluent.kafka.connect.cdc;

import com.google.common.base.Joiner;
import io.confluent.kafka.connect.utils.config.ConfigUtils;
import io.confluent.kafka.connect.utils.config.ValidEnum;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Range;
import org.apache.kafka.common.config.ConfigDef.Type;

import java.util.Map;

public class CDCSourceConnectorConfig extends AbstractConfig {
  public static final String TOPIC_FORMAT_CONFIG = "topicFormat.format";
  public static final String NAMESPACE_CONFIG = "schema.namespace.format";
  public static final String KEY_NAME_FORMAT_CONFIG = "schema.key.name.format";
  public static final String VALUE_NAME_FORMAT_CONFIG = "schema.value.name.format";
  public static final String SCHEMA_CACHE_MS_CONFIG = "schema.cache.ms";
  public static final String BATCH_SIZE_CONFIG = "batch.size";
  public static final String BACKOFF_TIME_MS_CONFIG = "backoff.time.ms";
  static final String TEMPLATE_VARIABLES = "`" + Joiner.on("`, `").join(Constants.TEMPLATE_VARIABLES) + "`";
  static final String TOPIC_FORMAT_DOC = "The topicFormat to write the data to.";
  static final String NAMESPACE_DOC = "The namespace for the schemas generated by the connector. The following template " +
      "properties are available for string replacement. " + TEMPLATE_VARIABLES;
  static final String KEY_NAME_FORMAT_DOC = "Format used to generate the name for the key schema. The following template " +
      "properties are available for string replacement. " + TEMPLATE_VARIABLES;
  static final String VALUE_NAME_FORMAT_DOC = "Format used to generate the name for the value schema. The following template " +
      "properties are available for string replacement. " + TEMPLATE_VARIABLES;
  static final String SCHEMA_CACHE_MS_DOC = "The number of milliseconds to cache schema metadata in memory.";
  static final String BATCH_SIZE_DOC = "The number of records to return in a batch.";
  static final String BACKOFF_TIME_MS_DOC = "The number of milliseconds to wait when no records are returned.";
  static final String SCHEMA_CASE_FORMAT = "schema.caseformat";
  public static final String SCHEMA_CASE_FORMAT_DATABASE_NAMES_CONFIG = SCHEMA_CASE_FORMAT + ".database.name";
  public static final String SCHEMA_CASE_FORMAT_SCHEMA_NAMES_CONFIG = SCHEMA_CASE_FORMAT + ".schema.name";
  public static final String SCHEMA_CASE_FORMAT_TABLE_NAMES_CONFIG = SCHEMA_CASE_FORMAT + ".table.name";
  public static final String SCHEMA_CASE_FORMAT_COLUMN_NAMES_CONFIG = SCHEMA_CASE_FORMAT + ".column.name";
  public static final String SCHEMA_CASE_FORMAT_INPUT_CONFIG = SCHEMA_CASE_FORMAT + ".input";

  static final String FORMATTED_SETTINGS = "`" + Joiner.on("`, `").join(NAMESPACE_CONFIG, KEY_NAME_FORMAT_CONFIG, VALUE_NAME_FORMAT_CONFIG, TOPIC_FORMAT_CONFIG) + "`";
  static final String SCHEMA_CASE_FORMAT_INPUT_DOC = "The naming convention used by the database format. " +
      "This is used to define the source naming convention used by the other `" + SCHEMA_CASE_FORMAT + ".*` properties.";
  static final String SCHEMA_CASE_FORMAT_DATABASE_NAMES_DOC = "This setting is used to control how the `${databaseName}` " +
      "variable is cased when it is passed to the formatters defined in the " + FORMATTED_SETTINGS + " settings. " +
      "This allows you to control the naming applied to these properties. For example this can be used to take a " +
      "database name of `USER_TRACKING` to a more java like case of `userTracking` or all lowercase `usertracking`.";
  static final String SCHEMA_CASE_FORMAT_SCHEMA_NAMES_DOC = "This setting is used to control how the `${schemaName}` " +
      "variable is cased when it is passed to the formatters defined in the " + FORMATTED_SETTINGS + " settings. " +
      "This allows you to control the naming applied to these properties. For example this can be used to take a " +
      "schema name of `SCOTT` to a more java like case of `Scott` or all lowercase `scott`.";
  static final String SCHEMA_CASE_FORMAT_TABLE_NAMES_DOC = "This setting is used to control how the `${tableName}` " +
      "variable is cased when it is passed to the formatters defined in the " + FORMATTED_SETTINGS + " settings. " +
      "This allows you to control the naming applied to these properties. For example this can be used to take a " +
      "table name of `USER_SETTING` to a more java like case of `UserSetting` or all lowercase `usersetting`.";
  static final String SCHEMA_CASE_FORMAT_COLUMN_NAMES_DOC = "This setting is used to control how the column names are " +
      "cased when the resulting schemas are generated.";
  public final String namespace;
  public final String topicFormat;
  public final String keyNameFormat;
  public final String valueNameFormat;
  public final int batchSize;
  public final int backoffTimeMs;
  public final int schemaCacheMs;

  public final CaseFormat schemaInputFormat;
  public final CaseFormat schemaDatabaseNameFormat;
  public final CaseFormat schemaSchemaNameFormat;
  public final CaseFormat schemaTableNameFormat;
  public final CaseFormat schemaColumnNameFormat;

  public CDCSourceConnectorConfig(ConfigDef definition, Map<?, ?> originals) {
    super(definition, originals);
    this.namespace = this.getString(NAMESPACE_CONFIG);
    this.keyNameFormat = this.getString(KEY_NAME_FORMAT_CONFIG);
    this.valueNameFormat = this.getString(VALUE_NAME_FORMAT_CONFIG);
    this.batchSize = this.getInt(BATCH_SIZE_CONFIG);
    this.backoffTimeMs = this.getInt(BACKOFF_TIME_MS_CONFIG);
    this.schemaCacheMs = this.getInt(SCHEMA_CACHE_MS_CONFIG);
    this.topicFormat = this.getString(TOPIC_FORMAT_CONFIG);

    this.schemaInputFormat = ConfigUtils.getEnum(CaseFormat.class, this, SCHEMA_CASE_FORMAT_INPUT_CONFIG);
    this.schemaDatabaseNameFormat = ConfigUtils.getEnum(CaseFormat.class, this, SCHEMA_CASE_FORMAT_DATABASE_NAMES_CONFIG);
    this.schemaSchemaNameFormat = ConfigUtils.getEnum(CaseFormat.class, this, SCHEMA_CASE_FORMAT_SCHEMA_NAMES_CONFIG);
    this.schemaTableNameFormat = ConfigUtils.getEnum(CaseFormat.class, this, SCHEMA_CASE_FORMAT_TABLE_NAMES_CONFIG);
    this.schemaColumnNameFormat = ConfigUtils.getEnum(CaseFormat.class, this, SCHEMA_CASE_FORMAT_COLUMN_NAMES_CONFIG);
  }

  public static ConfigDef config() {
    return new ConfigDef()
        .define(TOPIC_FORMAT_CONFIG, Type.STRING, "${databaseName}.${tableName}", Importance.HIGH, TOPIC_FORMAT_DOC)
        .define(NAMESPACE_CONFIG, Type.STRING, "com.example.data.${databaseName}", Importance.HIGH, NAMESPACE_DOC)


        .define(KEY_NAME_FORMAT_CONFIG, Type.STRING, "${namespace}.${tableName}Key", Importance.HIGH, KEY_NAME_FORMAT_DOC)
        .define(VALUE_NAME_FORMAT_CONFIG, Type.STRING, "${namespace}.${tableName}Value", Importance.HIGH, VALUE_NAME_FORMAT_DOC)
        .define(BATCH_SIZE_CONFIG, Type.INT, 512, Range.atLeast(1), Importance.LOW, BATCH_SIZE_DOC)

        .define(BACKOFF_TIME_MS_CONFIG, Type.INT, 1000, Range.atLeast(50), Importance.LOW, BACKOFF_TIME_MS_DOC)
        .define(SCHEMA_CACHE_MS_CONFIG, Type.INT, 5 * 60 * 1000, Range.atLeast(60000), Importance.LOW, SCHEMA_CACHE_MS_DOC)

        .define(SCHEMA_CASE_FORMAT_INPUT_CONFIG, Type.STRING, CaseFormat.UPPER_UNDERSCORE.toString(), ValidEnum.of(CaseFormat.class, CaseFormat.LOWER.toString(), CaseFormat.UPPER.toString(), CaseFormat.NONE.toString()), Importance.LOW, SCHEMA_CASE_FORMAT_INPUT_DOC)
        .define(SCHEMA_CASE_FORMAT_DATABASE_NAMES_CONFIG, Type.STRING, CaseFormat.NONE.name(), ValidEnum.of(CaseFormat.class), Importance.LOW, SCHEMA_CASE_FORMAT_DATABASE_NAMES_DOC)
        .define(SCHEMA_CASE_FORMAT_SCHEMA_NAMES_CONFIG, Type.STRING, CaseFormat.NONE.name(), ValidEnum.of(CaseFormat.class), Importance.LOW, SCHEMA_CASE_FORMAT_SCHEMA_NAMES_DOC)
        .define(SCHEMA_CASE_FORMAT_TABLE_NAMES_CONFIG, Type.STRING, CaseFormat.NONE.name(), ValidEnum.of(CaseFormat.class), Importance.LOW, SCHEMA_CASE_FORMAT_TABLE_NAMES_DOC)
        .define(SCHEMA_CASE_FORMAT_COLUMN_NAMES_CONFIG, Type.STRING, CaseFormat.NONE.name(), ValidEnum.of(CaseFormat.class), Importance.LOW, SCHEMA_CASE_FORMAT_COLUMN_NAMES_DOC)
        ;
  }

  public enum CaseFormat {
    LOWER_HYPHEN,
    LOWER_UNDERSCORE,
    LOWER_CAMEL,
    LOWER,
    UPPER_CAMEL,
    UPPER_UNDERSCORE,
    UPPER,
    NONE
  }

}
