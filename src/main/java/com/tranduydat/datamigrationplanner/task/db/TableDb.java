package com.tranduydat.datamigrationplanner.task.db;

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
public class TableDb {
  private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger(TableDb.class);
  @Getter
  @Setter
  @NonNull
  private Connection conn;

  public List<SchemaTableModel> getTableNamesByDb(String targetDb) {
    String query = String.format("SELECT sc.name AS schemaName, ta.name AS tableName" +
      " FROM [%s].sys.tables AS ta" +
      " INNER JOIN [%s].sys.partitions pa ON pa.OBJECT_ID = ta.OBJECT_ID" +
      " INNER JOIN [%s].sys.schemas sc ON ta.schema_id = sc.schema_id" +
      " WHERE ta.is_ms_shipped = 0 AND pa.index_id IN (1, 0)", targetDb, targetDb, targetDb);

    List<SchemaTableModel> tableNames = new ArrayList<>();

    try (PreparedStatement ps = conn.prepareStatement(query)) {
      logger.debug("" + ps.toString());
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          tableNames.add(SchemaTableModel.builder()
            .schemaName(rs.getString("schemaName"))
            .tableName(rs.getString("tableName"))
            .build());
        }
      }
    } catch (SQLException e) {
      logger.error("target_db={}, msg='Failed to get table names'", targetDb, e);
    }

    return tableNames;
  }
}
