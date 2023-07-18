package com.tranduydat.datamigrationplanner.model;

import lombok.*;

/**
 * To store schema and table info
 * DbModel -> TableModel
 *
 * @author Dat Tran (dattd6)
 * @version 1
 * @since 2023/07/13
 */
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class SchemaTableModel {
  @Setter
  @Getter
  private String schemaName;
  @Setter
  @Getter
  private String tableName;
}
