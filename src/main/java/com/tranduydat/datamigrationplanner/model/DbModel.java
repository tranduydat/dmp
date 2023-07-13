package com.tranduydat.datamigrationplanner.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

import java.util.Map;

/**
 * DbModel -> SchemaModel -> TableModel
 *
 * @author Dat Tran (dattd6)
 * @version 1
 * @since 2023/07/13
 */
@AllArgsConstructor
@Builder
public class DbModel {
  @Getter
  @Setter
  private String dbName;
  @Getter
  @Setter
  private Map<String, TableModel> tableMap;
}
