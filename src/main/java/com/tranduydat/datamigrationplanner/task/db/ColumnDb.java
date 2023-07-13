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
public class ColumnDb {
  private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger(ColumnDb.class);
  @Getter
  @Setter
  @NonNull
  private Connection conn;

  public List<String> getAllColumnsOfTable(String dbName, String schemaName, String tableName) throws SQLException {
    String query = String.format("SELECT COLUMN_NAME" +
      " FROM [%s].INFORMATION_SCHEMA.COLUMNS" +
      " WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s'", dbName, schemaName, tableName);
    List<String> columns = new ArrayList<>();

    try (PreparedStatement ps = conn.prepareStatement(query)) {
      logger.debug("" + ps);
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          columns.add(rs.getString("COLUMN_NAME"));
        }
      }
    } catch (SQLException e) {
      logger.error("db={}, table={}, msg='Failed to get all columns'", dbName, tableName, e);
    }

    return columns;
  }
}
