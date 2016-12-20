package io.confluent.kafka.connect.cdc;

import com.google.common.base.CaseFormat;
import com.google.common.base.Strings;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import freemarker.cache.StringTemplateLoader;
import freemarker.core.InvalidReferenceException;
import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateException;
import freemarker.template.TemplateExceptionHandler;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.DataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

class SchemaGenerater {
  public static final String METADATA_FIELD = "_cdc_metadata";
  private static final Logger log = LoggerFactory.getLogger(SchemaGenerater.class);
  final CDCSourceConnectorConfig config;
  final Cache<ChangeKey, SchemaPair> schemaPairCache;
  final Configuration configuration;
  final StringTemplateLoader loader;

  final Template namespaceTemplate;
  final Template keyTemplate;
  final Template valueTemplate;


  public SchemaGenerater(CDCSourceConnectorConfig config) {
    this.config = config;
    this.schemaPairCache = CacheBuilder.newBuilder()
        .expireAfterWrite(this.config.schemaCacheMs, TimeUnit.MILLISECONDS)
        .build();

    this.configuration = new Configuration(Configuration.getVersion());
    this.loader = new StringTemplateLoader();
    this.configuration.setTemplateLoader(this.loader);
    this.configuration.setDefaultEncoding("UTF-8");
    this.configuration.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);
    this.configuration.setLogTemplateExceptions(false);

    this.namespaceTemplate = loadTemplate(CDCSourceConnectorConfig.NAMESPACE_CONFIG, this.config.namespace);
    this.keyTemplate = loadTemplate(CDCSourceConnectorConfig.KEY_NAME_FORMAT_CONFIG, this.config.keyNameFormat);
    this.valueTemplate = loadTemplate(CDCSourceConnectorConfig.VALUE_NAME_FORMAT_CONFIG, this.config.valueNameFormat);
  }

  final Template loadTemplate(String templateName, String template) {
    if (log.isDebugEnabled()) {
      log.debug("Adding templateName '{}' template '{}'", templateName, template);
    }
    this.loader.putTemplate(templateName, template);
    try {
      if (log.isInfoEnabled()) {
        log.info("Loading template '{}'", templateName);
      }
      return this.configuration.getTemplate(templateName);
    } catch (IOException ex) {
      throw new DataException(
          String.format("Exception thrown while loading template '%s'", templateName),
          ex
      );
    }
  }

  Map<String, String> values(Change change, String namespace) {
    Map<String, String> values = new HashMap<>();
    if (!Strings.isNullOrEmpty(namespace)) {
      values.put("namespace", namespace);
    }

    values.put("schemaName", change.schemaName().toLowerCase());
    values.put("tableName", CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, change.tableName()));
    return values;
  }

  String renderTemplate(Change change, Template template, String namespace) {
    Map<String, String> value = values(change, namespace);

    try (StringWriter writer = new StringWriter()) {
      template.process(value, writer);
      return writer.toString();
    } catch (IOException e) {
      throw new DataException("Exception while processing template", e);
    } catch (InvalidReferenceException e) {
      throw new DataException(
          String.format(
              "Exception thrown while processing template. Offending expression '%s'",
              e.getBlamedExpressionString()
          ),
          e);
    } catch (TemplateException e) {
      throw new DataException("Exception while processing template", e);
    }
  }

  String keySchemaName(Change change) {
    String namespace = namespace(change);
    return renderTemplate(change, this.keyTemplate, namespace);
  }

  String valueSchemaName(Change change) {
    String namespace = namespace(change);
    return renderTemplate(change, this.valueTemplate, namespace);
  }

  String namespace(Change change) {
    return renderTemplate(change, this.namespaceTemplate, null);
  }

  String fieldName(Change.ColumnValue columnValue) {
    String fieldName = CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnValue.columnName());
    return fieldName;
  }

  void addFields(List<Change.ColumnValue> columnValues, List<String> fieldNames, SchemaBuilder builder) {
    for (Change.ColumnValue columnValue : columnValues) {
      String fieldName = fieldName(columnValue);
      fieldNames.add(fieldName);
      builder.field(fieldName, columnValue.schema());
    }
  }

  Schema generateValueSchema(Change change, List<String> schemaFields) {
    SchemaBuilder builder = SchemaBuilder.struct();
    String schemaName = valueSchemaName(change);
    builder.name(schemaName);
    addFields(change.valueColumns(), schemaFields, builder);
    builder.field(METADATA_FIELD, SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA));
    return builder.build();
  }

  Schema generateKeySchema(Change change, List<String> schemaFields) {
    SchemaBuilder builder = SchemaBuilder.struct();
    String schemaName = keySchemaName(change);
    builder.name(schemaName);
    addFields(change.keyColumns(), schemaFields, builder);
    return builder.build();
  }


  SchemaPair generateSchemas(Change change) {
    List<String> keySchemaFields = new ArrayList<>();
    Schema keySchema = generateKeySchema(change, keySchemaFields);
    List<String> valueSchemaFields = new ArrayList<>();
    Schema valueSchema = generateValueSchema(change, valueSchemaFields);
    return new SchemaPair(
        new SchemaAndFields(keySchema, keySchemaFields),
        new SchemaAndFields(valueSchema, valueSchemaFields)
    );
  }

  public SchemaPair get(final Change change) {
    ChangeKey changeKey = new ChangeKey(change);
    SchemaPair result = null;
    try {
      result = this.schemaPairCache.get(changeKey, new Callable<SchemaPair>() {
        @Override
        public SchemaPair call() throws Exception {
          return generateSchemas(change);
        }
      });
    } catch (ExecutionException e) {
      throw new DataException("Exception thrown while building schemas.", e);
    }

    return result;
  }

}
