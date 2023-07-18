package com.tranduydat.datamigrationplanner.db.connection;

import com.tranduydat.datamigrationplanner.config.Config;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.logging.log4j.LogManager;

/**
 * To obtain database connection pool
 * from Microsoft SQL Server by using HikariCP
 *
 * @author Dat Tran (dattd6)
 * @version 1
 * @since 2023/07/13
 */
public class MsSqlServerConnection {
  private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger(MsSqlServerConnection.class);
  private static final Config config = Config.getInstance();
  private static HikariDataSource hikariDataSource;

  private MsSqlServerConnection() {
    // Private constructor to enforce Singleton pattern
  }

  /**
   * Retrieves the singleton instance of HikariDataSource.
   *
   * @return HikariDataSource instance
   */
  public static synchronized HikariDataSource getInstance() {
    if (hikariDataSource == null) {
      try {
        Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
      } catch (ClassNotFoundException e) {
        throw new RuntimeException(e);
      }

      // Constructing the JDBC URI for connecting to the Microsoft SQL Server
      String jdbcUri = String.format("jdbc:sqlserver://%s:%d;encrypt=true;trustServerCertificate=true;databaseName=%s",
        config.getMssqlServerHost(),
        config.getMssqlServerPort(),
        config.getMssqlServerDb());

      // Creating a new HikariConfig object and configuring it
      HikariConfig hikariConfig = new HikariConfig();
      hikariConfig.setJdbcUrl(jdbcUri);
      hikariConfig.setUsername(config.getMssqlServerUsername());
      hikariConfig.setPassword(config.getMssqlServerPassword());
      hikariConfig.setMinimumIdle(config.getCpMinIdle());
      hikariConfig.setMaximumPoolSize(config.getCpMaxSize());

      // Creating a new HikariDataSource instance with the configured HikariConfig
      hikariDataSource = new HikariDataSource(hikariConfig);
    }
    return hikariDataSource;
  }
}
