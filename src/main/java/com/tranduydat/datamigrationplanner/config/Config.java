package com.tranduydat.datamigrationplanner.config;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @author Dat Tran (dattd6)
 * @version 1
 * @since 2023/07/13
 */

@Data
public class Config {
  private static Config instance;

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

  private String mssqlServerHost;
  private int mssqlServerPort;
  private String mssqlServerDb;
  private String mssqlServerUsername;
  private String mssqlServerPassword;
  private String mssqlServerJdbcUri = String.format("jdbc:sqlserver://%s:%d;encrypt=true;trustServerCertificate=true;databaseName=%s",
    mssqlServerHost, mssqlServerPort, mssqlServerDb);
  private int cpMaxSize = 5;
  private int cpMinIdle = 1;
  private String savePath;
}
