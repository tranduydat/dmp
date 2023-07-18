package com.tranduydat.datamigrationplanner.task;

import com.tranduydat.datamigrationplanner.db.connection.MsSqlServerConnection;
import com.tranduydat.datamigrationplanner.db.dao.CheckingDao;
import com.tranduydat.datamigrationplanner.db.dao.TableDao;
import com.tranduydat.datamigrationplanner.model.DbModel;
import com.tranduydat.datamigrationplanner.model.SchemaTableModel;
import com.tranduydat.datamigrationplanner.model.TableModel;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * To retrieve necessary database's info
 *
 * @author Dat Tran (dattd6)
 * @version 0.1
 * @since 2023/07/13
 */
public class DbReader {
  private static final Logger logger = LogManager.getLogger(DbReader.class);

  /**
   * Retrieves the list of columns for the given table.
   *
   * @param tableDao   The TableDao instance.
   * @param dbName     The name of the database.
   * @param schemaName The name of the schema.
   * @param tableName  The name of the table.
   * @return The list of column names.
   */
  private static List<String> getColumns(TableDao tableDao,
                                         String dbName,
                                         String schemaName,
                                         String tableName) {
    List<String> columns = null;
    try {
      columns = tableDao.getAllColumnsOfTable(dbName, schemaName, tableName);
    } catch (SQLException e) {
      logger.error("db={},schema={},table={},msg=Failed to get list of columns,e={}", dbName, schemaName, tableName, e.getMessage(), e);
    }
    logger.info("db={},schema={},table={},number_of_columns={},columns={}", dbName, schemaName, tableName, columns.size(), columns);
    return columns;
  }

  public DbModel get(String dbName) throws SQLException {
    logger.info("db={},msg=Starting DbTask", dbName);

    // Try to get connection pool
    try (Connection conn = MsSqlServerConnection.getInstance().getConnection()) {
      TableDao tableDao = new TableDao(conn);
      CheckingDao checkingDao = new CheckingDao(conn);
      DbModel dbModel = DbModel.builder()
        .dbName(dbName)
        .tableMap(new HashMap<>())
        .build();

      // Get all tables in a database
      List<SchemaTableModel> tables = this.getTables(dbName, tableDao);

      // Iterate over all tables
      tables.parallelStream().forEach(schemaTableModel -> {
        String schemaName = schemaTableModel.getSchemaName();
        String tableName = schemaTableModel.getTableName();

        // 1. Get primary keys
        // In the case of a composite site key,
        // it cannot guarantee that one of the columns is unique,
        // and Sqoop does not support this type of key in `split-by`.
        // Therefore, besides retrieving primary keys, it is necessary to further consider other columns.
        List<String> primaryKeys = this.getPrimaryKey(checkingDao, dbName, schemaName, tableName);

        // 2. Get total count of a table
        int totalCount = this.getCountTotal(checkingDao, dbName, schemaName, tableName);

        // 3. Get all columns in a table
        List<String> columns = getColumns(tableDao, dbName, schemaName, tableName);

        // 4. Get count distinct for each column
        // KNOWN ISSUE: `ntext` data type does not support (distinct) count, then skip
        Map<String, Integer> countDistinctColumnMap = this.getCountDistinctColumns(checkingDao, dbName, schemaName, tableName, columns);

        // Build table model with all retrieved info
        TableModel tableModel = TableModel.builder()
          .totalCount(totalCount)
          .primaryKeys(primaryKeys)
          .columnWithCount(countDistinctColumnMap)
          .build();

        // Put table model to database model (a database contains multiple tables)
        dbModel.getTableMap().put("[" + schemaName + "].[" + tableName + "]", tableModel);
      });

      return dbModel;
    }
  }

  /**
   * Retrieves the list of tables for the given database name.
   *
   * @param dbName   The name of a database.
   * @param tableDao The TableDao instance.
   * @return The list of SchemaTableModel representing the tables (and its schema) in a database.
   */
  private List<SchemaTableModel> getTables(String dbName, TableDao tableDao) {
    List<SchemaTableModel> tables = tableDao.getTableNamesByDb(dbName);
    logger.info("db={},count_table={},tables={}", dbName, tables.size(), tables);
    logger.info("db={},msg=Starting processing each table", dbName);
    return tables;
  }

  /**
   * Retrieves the distinct count of columns for the given table.
   *
   * @param checkingDao The CheckingDao instance.
   * @param dbName      The name of the database.
   * @param schemaName  The name of the schema.
   * @param tableName   The name of the table.
   * @param columns     The list of column names.
   * @return A map of column names and their distinct counts.
   */
  private Map<String, Integer> getCountDistinctColumns(CheckingDao checkingDao,
                                                       String dbName,
                                                       String schemaName,
                                                       String tableName,
                                                       List<String> columns) {
    Map<String, Integer> distinctColumnByColumnMap = new HashMap<>();
    for (String columnName : columns) {
      // Count distinct row by a column
      Integer distinctCount = null;
      try {
        distinctCount = checkingDao.countDistinctByColumn(dbName, schemaName, tableName, columnName);
      } catch (SQLException e) {
        logger.error("db={},schema={},table={},msg=Failed to get distinct count by column,e={}", dbName, schemaName, tableName, e.getMessage(), e);
      }
      // Put a column with its distinct count
      distinctColumnByColumnMap.put(columnName, distinctCount);
    }
    logger.info("db={},schema={},table={},column_with_distinct_count={}", dbName, schemaName, tableName, distinctColumnByColumnMap);
    return distinctColumnByColumnMap;
  }

  /**
   * Retrieves the total count of rows for the given table.
   *
   * @param checkingDao The CheckingDao instance.
   * @param dbName      The name of the database.
   * @param schemaName  The name of the schema.
   * @param tableName   The name of the table.
   * @return The total count of rows.
   */
  private int getCountTotal(CheckingDao checkingDao, String dbName, String schemaName, String tableName) {
    int totalCount = -1;
    try {
      totalCount = checkingDao.countRowByColumn(dbName, schemaName, tableName);
    } catch (SQLException e) {
      logger.error("db={},schema={},table={},msg=Failed to get total count rows,e={}", dbName, schemaName, tableName, e.getMessage(), e);
    }
    logger.info("db={},schema={},table={},count_total={}", dbName, schemaName, tableName, totalCount);
    return totalCount;
  }

  /**
   * Retrieves the primary keys for the given table.
   *
   * @param checkingDao The CheckingDao instance.
   * @param dbName      The name of the database.
   * @param schemaName  The name of the schema.
   * @param tableName   The name of the table.
   * @return The list of primary keys.
   */
  private List<String> getPrimaryKey(CheckingDao checkingDao, String dbName, String schemaName, String tableName) {
    List<String> primaryKeys = null;
    try {
      primaryKeys = checkingDao.getPrimaryKeys(dbName, schemaName, tableName);
    } catch (SQLException e) {
      logger.error("db={},schema={},table={},msg=Failed to get primary keys,e={}", dbName, schemaName, tableName, e.getMessage(), e);
    }
    logger.info("db={},schema={},table={},primary_keys={}", dbName, schemaName, tableName, primaryKeys);
    return primaryKeys;
  }
}
