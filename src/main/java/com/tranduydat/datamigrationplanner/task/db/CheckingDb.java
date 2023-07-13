package com.tranduydat.datamigrationplanner.task.db;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.apache.logging.log4j.LogManager;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Dat Tran (dattd6)
 * @version 1
 * @since 2023/07/13
 */
@AllArgsConstructor
public class CheckingDb {
  private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger(CheckingDb.class);
  @Getter
  @Setter
  @NonNull
  private Connection conn;

  public int countDistinctRowByColumn(String dbName, String schemaName, String tableName, String columnName) throws SQLException {
    String query = String.format("SELECT COUNT(DISTINCT [%s]) AS count_rows FROM [%s].[%s].[%s]", columnName, dbName, schemaName, tableName);
    int numberOfRows = -1;
    try (PreparedStatement ps = conn.prepareStatement(query)) {
      logger.debug("" + ps);
      try (ResultSet rs = ps.executeQuery()) {
        if (rs.next()) {
          numberOfRows = rs.getInt("count_rows");
        }
      }
    } catch (SQLException e) {
      logger.error("db={}, schema={}, table={}, column={}, msg='Failed to run count distinct row'",
        dbName, schemaName, tableName, columnName, e);
    }
    return numberOfRows;
  }

  public int countRowByColumn(String dbName, String schemaName, String tableName) throws SQLException {
    String query = String.format("SELECT COUNT(*) AS count_rows FROM [%s].[%s].[%s]", dbName, schemaName, tableName);
    int numberOfRows = -1;
    try (PreparedStatement ps = conn.prepareStatement(query)) {
      logger.debug("" + ps);
      try (ResultSet rs = ps.executeQuery()) {
        if (rs.next()) {
          numberOfRows = rs.getInt("count_rows");
        }
      }
    } catch (SQLException e) {
      logger.error("db={}, schema={}, table={}, msg='Failed to run count row by column'",
        dbName, schemaName, tableName, e);
    }
    return numberOfRows;
  }

  public List<String> getPrimaryKeys(String dbName, String schemaName, String tableName) throws SQLException {
    String query = String.format("SELECT COLUMN_NAME" +
      " FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE" +
      " WHERE OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA + '.' + CONSTRAINT_NAME), 'IsPrimaryKey') = 1" +
      " AND TABLE_SCHEMA = '%s'" +
      " AND TABLE_NAME = '%s'", schemaName, tableName);
    List<String> primaryKeys = new ArrayList<>();
    try (PreparedStatement ps = conn.prepareStatement(query)) {
      logger.debug("" + ps);
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          primaryKeys.add(rs.getString("COLUMN_NAME"));
        }
      }
    } catch (SQLException e) {
      logger.error("db={}, schema={}, table={}, msg='Failed to run get primary keys'",
        dbName, schemaName, tableName, e);
    }
    return primaryKeys;
  }
}
