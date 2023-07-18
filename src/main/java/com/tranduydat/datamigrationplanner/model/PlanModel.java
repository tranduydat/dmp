package com.tranduydat.datamigrationplanner.model;

import lombok.*;

import java.util.List;

/**
 * To store Plan info
 * A plan can have multiple plan details
 *
 * @author Dat Tran (dattd6)
 * @version 1
 * @since 2023/07/13
 */
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class PlanModel {
  @Getter
  @Setter
  private String dbName;
  @Getter
  @Setter
  private Integer numberOfTables;
  @Getter
  @Setter
  private List<PlanDetailModel> details;
}
