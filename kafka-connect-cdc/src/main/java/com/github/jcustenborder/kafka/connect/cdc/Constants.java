/**
 * Copyright © 2017 Jeremy Custenborder (jcustenborder@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.jcustenborder.kafka.connect.cdc;

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
