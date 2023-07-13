package com.tranduydat.datamigrationplanner.task.process;

import com.tranduydat.datamigrationplanner.model.DbModel;
import com.tranduydat.datamigrationplanner.model.SchemaTableModel;
import com.tranduydat.datamigrationplanner.model.TableModel;
import com.tranduydat.datamigrationplanner.task.connection.MsSqlServerConnection;
import com.tranduydat.datamigrationplanner.task.db.CheckingDb;
import com.tranduydat.datamigrationplanner.task.db.ColumnDb;
import com.tranduydat.datamigrationplanner.task.db.TableDb;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Dat Tran (dattd6)
 * @version 0.1
 * @since 2023/07/13
 */
public class DbProcess {
  private static final Logger logger = LogManager.getLogger(DbProcess.class);

  public DbModel get(String dbName) throws SQLException {
    Connection conn = null;

    try {
      // Obtain database connection pool
      conn = MsSqlServerConnection.getInstance().getConnection();

      TableDb tableDb = new TableDb(conn);
      ColumnDb columnDb = new ColumnDb(conn);
      CheckingDb checkingDb = new CheckingDb(conn);
      DbModel dbModel = DbModel.builder()
        .dbName(dbName)
        .tableMap(new HashMap<>())
        .build();

      logger.info("db={},msg=Starting DbTask", dbName);
      List<SchemaTableModel> tables = tableDb.getTableNamesByDb(dbName);
      logger.info("db={},count_table={},tables={}", dbName, tables.size(), tables);
      logger.info("db={},msg=Starting processing each table", dbName);

      tables.parallelStream().forEach(schemaTableModel -> {
        String schemaName = schemaTableModel.getSchemaName();
        String tableName = schemaTableModel.getTableName();

        // Get all primary keys in db
        List<String> primaryKeys = null;
        try {
          primaryKeys = checkingDb.getPrimaryKeys(dbName, schemaName, tableName);
        } catch (SQLException e) {
          logger.error("db={},schema={},table={},msg=Failed to get primary keys,e={}", dbName, schemaName, tableName, e.getMessage(), e);
        }
        logger.info("db={},schema={},table={},primary_keys={}", dbName, schemaName, tableName, primaryKeys);
        TableModel tableModel = new TableModel();

        // Get count of all rows
        int totalCount = -1;
        try {
          totalCount = checkingDb.countRowByColumn(dbName, schemaName, tableName);
        } catch (SQLException e) {
          logger.error("db={},schema={},table={},msg=Failed to get total count rows,e={}", dbName, schemaName, tableName, e.getMessage(), e);
        }
        logger.info("db={},schema={},table={},count_total={}", dbName, schemaName, tableName, totalCount);

        // Get all columns in a table
        List<String> columns = null;
        try {
          columns = columnDb.getAllColumnsOfTable(dbName, schemaName, tableName);
        } catch (SQLException e) {
          logger.error("db={},schema={},table={},msg=Failed to get list of columns,e={}", dbName, schemaName, tableName, e.getMessage(), e);
        }
        logger.info("db={},schema={},table={},number_of_columns={},columns={}", dbName, schemaName, tableName, columns.size(), columns);

        // Parse each table
        Map<String, Integer> distinctColumnByColumnMap = new HashMap<>();
        for (String columnName : columns) {
          // Count distinct row by a column
          Integer distinctCount = null;
          try {
            distinctCount = checkingDb.countDistinctRowByColumn(dbName, schemaName, tableName, columnName);
          } catch (SQLException e) {
            logger.error("db={},schema={},table={},msg=Failed to get distinct count by column,e={}", dbName, schemaName, tableName, e.getMessage(), e);
          }

          distinctColumnByColumnMap.put(columnName, distinctCount);
        }
        logger.info("db={},schema={},table={},column_with_distinct_count={}", dbName, schemaName, tableName, distinctColumnByColumnMap);

        tableModel.setTotalCount(totalCount);
        tableModel.setPrimaryKeys(primaryKeys);
        tableModel.setColumnWithCount(distinctColumnByColumnMap);

        dbModel.getTableMap().put("[" + schemaName + "].[" + tableName + "]", tableModel);
      });

      return dbModel;
    } finally {
      if (conn != null) {
        conn.close();
      }
    }
  }
}
