package com.tranduydat.datamigrationplanner.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * To store table info
 * DbModel -> TableModel
 *
 * @author Dat Tran (dattd6)
 * @version 1
 * @since 2023/07/13
 */
@AllArgsConstructor
@Builder
public class TableModel {
  @Getter
  @Setter
  private Map<String, Integer> columnWithCount;
  @Getter
  @Setter
  private int totalCount;
  @Getter
  @Setter
  private List<String> primaryKeys;

  public TableModel() {
    this.columnWithCount = new HashMap<>();
    this.totalCount = 0;
    this.primaryKeys = new ArrayList<>();
  }
}
