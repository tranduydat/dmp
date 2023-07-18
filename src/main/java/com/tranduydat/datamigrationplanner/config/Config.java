package com.tranduydat.datamigrationplanner.config;

import lombok.Data;

/**
 * To store all configs of this project
 *
 * @author Dat Tran (dattd6)
 * @version 1
 * @since 2023/07/13
 */

@Data
public class Config {
  private static volatile Config instance;
  private String mssqlServerHost;
  private int mssqlServerPort;
  private String mssqlServerDb;
  private String mssqlServerUsername;
  private String mssqlServerPassword;
  private String mssqlServerJdbcUri = String.format("jdbc:sqlserver://%s:%d;encrypt=true;trustServerCertificate=true;databaseName=%s",
    mssqlServerHost, mssqlServerPort, mssqlServerDb);
  // The max size of connection pool in HikariCP
  private int cpMaxSize = 5;
  // The min number of idle connections in HikariCP
  private int cpMinIdle = 1;
  // The file path to save plan
  private String savePath;

  private Config() {
    // Private constructor to prevent direct instantiation
  }

  public static Config getInstance() {
    if (instance == null) {
      synchronized (Config.class) {
        if (instance == null) {
          instance = new Config();
        }
      }
    }
    return instance;
  }
}
