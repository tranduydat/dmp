package com.tranduydat.datamigrationplanner.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

/**
 *
 * @author Dat Tran (dattd6)
 * @version 1
 * @since 2023/07/13
 */
@AllArgsConstructor
@Builder
public class PlanDetailModel {
  @Getter
  @Setter
  private String tableName;
  @Getter
  @Setter
  private TableModel tableModel;
  @Getter
  @Setter
  private String uniqueColumn;
}
