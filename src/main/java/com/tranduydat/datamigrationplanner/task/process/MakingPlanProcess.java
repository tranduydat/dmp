package com.tranduydat.datamigrationplanner.task.process;

import com.tranduydat.datamigrationplanner.model.DbModel;
import com.tranduydat.datamigrationplanner.model.PlanDetailModel;
import com.tranduydat.datamigrationplanner.model.PlanModel;
import com.tranduydat.datamigrationplanner.model.TableModel;
import lombok.NoArgsConstructor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

// Iterate over all columns
// if primary key list is not empty
// if ID is not applicable, then get the first
// else if primary key is empty
// then find column having distinct count = total count
// otherwise return it with -1 (error signal)
@NoArgsConstructor
public class MakingPlanProcess {
  private static final Logger logger = LogManager.getLogger(MakingPlanProcess.class);

  private String checkColumnByTotalCount(TableModel tableModel) {
    String uniqueColumn = tableModel.getColumnWithCount()
      .entrySet().parallelStream()
      .filter(x -> x.getValue() == tableModel.getTotalCount())
      .findFirst()
      .map(Map.Entry::getKey)
      .orElse("-1");
    return uniqueColumn;
  }

  public PlanModel make(DbModel dbModel) {
    PlanModel planModel = PlanModel.builder().dbName(dbModel.getDbName()).numberOfTables(dbModel.getTableMap().size()).build();
    logger.info("db={},msg=Start making plan process", dbModel.getDbName());

    // Iterate over all tables in a database
    List<PlanDetailModel> planDetails = new ArrayList<>();
    for (Map.Entry<String, TableModel> entry : dbModel.getTableMap().entrySet()) {
      String tableName = entry.getKey();
      TableModel tableModel = entry.getValue();
      String uniqueColumn = "-1";

      if (tableModel.getPrimaryKeys().size() == 1) {
        uniqueColumn = tableModel.getPrimaryKeys().get(0);
      } else {
        uniqueColumn = this.checkColumnByTotalCount(tableModel);
      }

      planDetails.add(PlanDetailModel.builder()
        .tableName(tableName)
        .uniqueColumn(uniqueColumn)
        .tableModel(tableModel)
        .build());

      logger.info("db={}, schema_table={}, unique_column={}", dbModel.getDbName(), tableName, uniqueColumn);
    }

    planModel.setDetails(planDetails);
    return planModel;
  }
}
