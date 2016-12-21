package io.confluent.kafka.connect.cdc.mssql;

import com.google.common.base.Preconditions;
import db.migration.AbstractIUDJdbcMigration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

class V0_0_1_100__Binary extends AbstractIUDJdbcMigration {
  private static final Logger log = LoggerFactory.getLogger(V0_0_1_100__Binary.class);
//  CREATE TABLE [dbo].[binary_table] (
//  id         BIGINT IDENTITY PRIMARY KEY NOT NULL,
//      [value]    BINARY(128)
//);
//  CREATE TABLE [dbo].[varbinary_table] (
//  id         BIGINT IDENTITY PRIMARY KEY NOT NULL,
//      [value]    VARBINARY(256)
//);
//  CREATE TABLE [dbo].[image_table] (
//  id         BIGINT IDENTITY PRIMARY KEY NOT NULL,
//      [value]    IMAGE
//);

  void binary(Connection connection) throws SQLException {
    final String schemaName = "dbo";
    final String tableName = "binary_table";

    byte[] buffer = new byte[128];

    for (int i = 0; i < 5; i++) {
      Long id = null;
      try (PreparedStatement insert = insert(connection, schemaName, tableName)) {
        random.nextBytes(buffer);
        insert.setObject(1, buffer);
        insert.executeUpdate();
        try (ResultSet keys = insert.getGeneratedKeys()) {
          while (keys.next()) {
            id = keys.getLong(1);
          }
        }
      }

      try (PreparedStatement update = update(connection, schemaName, tableName)) {
        random.nextBytes(buffer);
        update.setObject(1, buffer);
        update.setLong(2, id);
        update.executeUpdate();
      }
    }
  }

  void addData(Connection connection, String schemaName, String tableName, Object... args) throws SQLException {
    Preconditions.checkState(args.length % 2 == 0, "args must be divisible by 2.");

    if(log.isInfoEnabled()){
      log.info("Adding {} records to [{}].[{}].", args.length, schemaName, tableName);
    }

    for (int i = 0; i < args.length; i += 2) {
      Object insertValue = args[i];
      Object updateValue = args[i+1];
      Long id = null;
      try (PreparedStatement insert = insert(connection, schemaName, tableName)) {
        insert.setObject(1, insertValue);
        insert.executeUpdate();
        try (ResultSet keys = insert.getGeneratedKeys()) {
          while (keys.next()) {
            id = keys.getLong(1);
          }
        }
      }

      try (PreparedStatement update = update(connection, schemaName, tableName)) {
        update.setObject(1, updateValue);
        update.setLong(2, id);
        update.executeUpdate();
      }
    }
  }

  @Override
  public void migrate(Connection connection) throws Exception {
    addData(connection, "dbo", "binary_table", randomBytes(128), randomBytes(128));
//    binary(connection);
  }
}
