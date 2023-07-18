package com.tranduydat.datamigrationplanner.task;

import com.tranduydat.datamigrationplanner.model.DbModel;
import com.tranduydat.datamigrationplanner.model.PlanDetailModel;
import com.tranduydat.datamigrationplanner.model.PlanModel;
import com.tranduydat.datamigrationplanner.model.TableModel;
import lombok.NoArgsConstructor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Making plan according to retrieved database's info
 *
 * @author Dat Tran (dattd6)
 * @version 0.1
 * @since 2023/07/13
 */
@NoArgsConstructor
public class MakingPlanProcessor {
  private static final Logger logger = LogManager.getLogger(MakingPlanProcessor.class);

  /**
   * Compares columns of a table model based on the total row count.
   * If a column's distinct count equals the total row count, it is considered unique.
   *
   * @param tableModel The table model.
   * @return The first column with its distinct count equal to the total count, otherwise returns "-1".
   */
  private String compareColumnsByTotalRow(TableModel tableModel) {
    return tableModel.getColumnWithCount()
      .entrySet().parallelStream()
      .filter(x -> x.getValue() == tableModel.getTotalCount())
      .findFirst()
      .map(Map.Entry::getKey)
      .orElse("-1");
  }

  /**
   * Determines the unique column of a table model.
   *
   * @param tableModel The table model.
   * @return The unique column.
   */
  private String determineUniqueColumn(TableModel tableModel) {
    // If Primary key is the only one key, it must be unique (not Composite primary key)
    String uniqueColumn = "-1";
    if (tableModel.getPrimaryKeys().size() == 1) {
      uniqueColumn = tableModel.getPrimaryKeys().get(0);
    } else {
      uniqueColumn = compareColumnsByTotalRow(tableModel);
    }

    // Reverse keyword in Sqoop
    if (uniqueColumn.equalsIgnoreCase("Key")) {
      uniqueColumn = "-1";
    }

    return uniqueColumn;
  }

  /**
   * Creates a plan detail model based on the provided table model entry.
   *
   * @param entry The table model entry.
   * @return The plan detail model.
   */
  private PlanDetailModel createPlanDetailModel(Map.Entry<String, TableModel> entry) {
    String tableName = entry.getKey();
    TableModel tableModel = entry.getValue();
    String uniqueColumn = determineUniqueColumn(tableModel);

    logger.info("db={}, schema_table={}, unique_column={}", tableName, tableName, uniqueColumn);

    return PlanDetailModel.builder()
      .tableName(tableName)
      .uniqueColumn(uniqueColumn)
      .tableModel(tableModel)
      .build();
  }

  /**
   * Builds a plan model with the provided parameters.
   *
   * @param dbName         The database name.
   * @param numberOfTables The number of tables.
   * @param planDetails    The list of plan detail models.
   * @return The plan model.
   */
  private PlanModel buildPlanModel(String dbName, int numberOfTables, List<PlanDetailModel> planDetails) {
    return PlanModel.builder()
      .dbName(dbName)
      .numberOfTables(numberOfTables)
      .details(planDetails)
      .build();
  }

  /**
   * Generates a plan model based on the provided database model.
   *
   * @param dbModel The database model
   * @return The plan model.
   */
  public PlanModel makeAndGet(DbModel dbModel) {
    logger.info("db={},msg=Start making plan process", dbModel.getDbName());

    // Generate plan details
    List<PlanDetailModel> planDetails = dbModel.getTableMap().entrySet().stream()
      .map(this::createPlanDetailModel)
      .collect(Collectors.toList());

    // Build and return the plan model
    return buildPlanModel(dbModel.getDbName(), dbModel.getTableMap().size(), planDetails);
  }
}
