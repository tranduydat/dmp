package com.tranduydat.datamigrationplanner.db.dao;

import com.tranduydat.datamigrationplanner.model.SchemaTableModel;
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
public class TableDao {
  private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger(TableDao.class);
  @Getter
  @Setter
  @NonNull
  private Connection conn;

  /**
   * To get all tables' name in a database
   *
   * @param dbName The name of the database.
   * @return The list of table names.
   */
  public List<SchemaTableModel> getTableNamesByDb(String dbName) {
    String query = String.format("SELECT sc.name AS schemaName, ta.name AS tableName" +
      " FROM [%s].sys.tables AS ta" +
      " INNER JOIN [%s].sys.partitions pa ON pa.OBJECT_ID = ta.OBJECT_ID" +
      " INNER JOIN [%s].sys.schemas sc ON ta.schema_id = sc.schema_id" +
      " WHERE ta.is_ms_shipped = 0 AND pa.index_id IN (1,0)" +
      " GROUP BY sc.name, ta.name" +
      " HAVING SUM(pa.rows) > 0" +
      " ORDER BY SUM(pa.rows) DESC", dbName, dbName, dbName);

    // The list of table names.
    List<SchemaTableModel> tableNames = new ArrayList<>();

    // Execute the query and get the list of table names.
    try (PreparedStatement ps = this.conn.prepareStatement(query)) {
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          tableNames.add(SchemaTableModel.builder()
            .schemaName(rs.getString("schemaName"))
            .tableName(rs.getString("tableName"))
            .build());
        }
      }
    } catch (SQLException e) {
      logger.error("target_db={},msg=Failed to get table names", dbName, e);
    }

    return tableNames;
  }

  /**
   * To get all columns of a table in a database
   *
   * @param dbName     The name of the database.
   * @param schemaName The name of the schema.
   * @param tableName  The name of the table.
   * @return The list of columns.
   * @throws SQLException If an error occurs.
   */
  public List<String> getAllColumnsOfTable(String dbName,
                                           String schemaName,
                                           String tableName)
    throws SQLException {
    String query = String.format("SELECT COLUMN_NAME" +
        " FROM [%s].INFORMATION_SCHEMA.COLUMNS" +
        " WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s'",
      dbName, schemaName, tableName);

    List<String> columns = new ArrayList<>();

    try (PreparedStatement ps = this.conn.prepareStatement(query)) {
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          columns.add(rs.getString("COLUMN_NAME"));
        }
      }
    } catch (SQLException e) {
      logger.error("db={},table={},msg=Failed to get all columns", dbName, tableName, e);
    }

    return columns;
  }
}
