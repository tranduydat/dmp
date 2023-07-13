package com.tranduydat.datamigrationplanner.task.connection;

import com.tranduydat.datamigrationplanner.config.Config;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.apache.logging.log4j.LogManager;

/**
 * To obtain database connection
 *
 * @author Dat Tran (dattd6)
 * @version 1
 * @since 2023/07/13
 */
public class MsSqlServerConnection {
  private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger(MsSqlServerConnection.class);
  private static HikariDataSource hikariDataSource;
  private static Config config = Config.getInstance();
  @Getter
  @Setter
  @NonNull
  private String db;

  private MsSqlServerConnection() {
    // Private constructor to enforce Singleton pattern
  }

  public static synchronized HikariDataSource getInstance() {
    if (hikariDataSource == null) {
      try {
        Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
      } catch (ClassNotFoundException e) {
        throw new RuntimeException(e);
      }
      HikariConfig hikariConfig = new HikariConfig();
      hikariConfig.setJdbcUrl(config.getMssqlServerJdbcUri());
      hikariConfig.setUsername(config.getMssqlServerUsername());
      hikariConfig.setPassword(config.getMssqlServerPassword());
      hikariConfig.setMinimumIdle(config.getCpMinIdle());
      hikariConfig.setMaximumPoolSize(config.getCpMaxSize());
      // Set other configuration properties as needed

      hikariDataSource = new HikariDataSource(hikariConfig);
    }
    return hikariDataSource;
  }
}
