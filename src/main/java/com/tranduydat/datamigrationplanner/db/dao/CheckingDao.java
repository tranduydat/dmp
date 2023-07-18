package com.tranduydat.datamigrationplanner.db.dao;

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
public class CheckingDao {
  private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger(CheckingDao.class);
  // The connection to database
  @Getter
  @Setter
  @NonNull
  private Connection conn;

  /**
   * To count the number of distinct rows in a column
   *
   * @param dbName     The name of the database.
   * @param schemaName The name of the schema.
   * @param tableName  The name of the table.
   * @param columnName The name of the column.
   * @return The number of distinct rows.
   * @throws SQLException If an error occurs.
   */
  public int countDistinctByColumn(String dbName,
                                   String schemaName,
                                   String tableName,
                                   String columnName)
    throws SQLException {
    String query = String.format("SELECT COUNT(DISTINCT [%s]) AS count_rows FROM [%s].[%s].[%s]",
      columnName, dbName, schemaName, tableName);

    int numberOfRows = -1;

    // Execute the query and get the number of rows.
    try (PreparedStatement ps = this.conn.prepareStatement(query)) {
      try (ResultSet rs = ps.executeQuery()) {
        if (rs.next()) {
          numberOfRows = rs.getInt("count_rows");
        }
      }
    } catch (SQLException e) {
      logger.error("db={},schema={},table={},column={},msg=Failed to run count distinct row",
        dbName, schemaName, tableName, columnName, e);
    }
    return numberOfRows;
  }

  /**
   * To count the number of rows in a table
   *
   * @param dbName     The name of the database.
   * @param schemaName The name of the schema.
   * @param tableName  The name of the table.
   * @return The number of rows.
   * @throws SQLException If an error occurs.
   */
  public int countRowByColumn(String dbName,
                              String schemaName,
                              String tableName)
    throws SQLException {
    String query = String.format("SELECT COUNT(*) AS count_rows FROM [%s].[%s].[%s]",
      dbName, schemaName, tableName);

    int numberOfRows = -1;

    // Execute the query and get the number of rows.
    try (PreparedStatement ps = this.conn.prepareStatement(query)) {
      try (ResultSet rs = ps.executeQuery()) {
        if (rs.next()) {
          numberOfRows = rs.getInt("count_rows");
        }
      }
    } catch (SQLException e) {
      logger.error("db={},schema={},table={},msg=Failed to run count row by column",
        dbName, schemaName, tableName, e);
    }
    return numberOfRows;
  }

  /**
   * To get (composite) primary keys of a table.
   *
   * @param dbName     The name of the database.
   * @param schemaName The name of the schema.
   * @param tableName  The name of the table.
   * @return The list of primary keys.
   * @throws SQLException If an error occurs.
   */
  public List<String> getPrimaryKeys(String dbName,
                                     String schemaName,
                                     String tableName)
    throws SQLException {
    String query = String.format("SELECT COLUMN_NAME" +
      " FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE" +
      " WHERE OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA + '.' + CONSTRAINT_NAME), 'IsPrimaryKey') = 1" +
      " AND TABLE_SCHEMA = '%s'" +
      " AND TABLE_NAME = '%s'", schemaName, tableName);

    // All (composite) primary keys in a table
    List<String> primaryKeys = new ArrayList<>();

    try (PreparedStatement ps = this.conn.prepareStatement(query)) {
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          primaryKeys.add(rs.getString("COLUMN_NAME"));
        }
      }
    } catch (SQLException e) {
      logger.error("db={},schema={},table={},msg=Failed to run get primary keys",
        dbName, schemaName, tableName, e);
    }
    return primaryKeys;
  }
}
