package com.tranduydat.datamigrationplanner.task;

import com.tranduydat.datamigrationplanner.model.PlanModel;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

/**
 * This class is responsible for writing the plan to a file.
 *
 * @author Dat Tran (dattd6)
 * @version 0.1
 * @since 2023/07/13
 */
public class PlanWriter {
  private static final Logger logger = LogManager.getLogger(PlanWriter.class);

  /**
   * Saves the plan to a file.
   *
   * @param saveFilePath The path of the file to save the plan to.
   * @param planModel    The PlanModel representing the plan.
   * @param delimiter    The delimiter to separate the table name and unique column in the file.
   */
  public void write(String saveFilePath, PlanModel planModel, String delimiter) {
    // Iterate over each PlanDetailModel in the planModel and write to the file
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(saveFilePath))) {
      planModel.getDetails().forEach(x -> {
        try {
          writer.write(x.getTableName() + delimiter + x.getUniqueColumn() + "\n");
        } catch (IOException e) {
          logger.error("db={},table={},unique_column={},save_file_path={},msg=Failed to write",
            planModel.getDbName(), x.getTableName(), x.getUniqueColumn(), saveFilePath);
          logger.error(e.getStackTrace());
        }
      });
    } catch (IOException e) {
      logger.fatal("db={},save_file_path={},msg=Failed to save file", planModel.getDbName(), saveFilePath);
      logger.fatal(e.getStackTrace());
    }
  }
}
