package com.tranduydat.datamigrationplanner.task.process;

import com.tranduydat.datamigrationplanner.model.PlanModel;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class SavingPlanProcess {
  private static final Logger logger = LogManager.getLogger(SavingPlanProcess.class);

  public void save(String saveFilePath, PlanModel planModel, String delimiter) {
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(saveFilePath))) {
      planModel.getDetails().stream().forEach(x -> {
        try {
          writer.write(x.getTableName() + delimiter + x.getUniqueColumn() + "\n");
        } catch (IOException e) {
          logger.error("db={},table={},unique_column={},save_file_path={},msg=Failed to write",
            planModel.getDbName(), x.getTableName(), x.getUniqueColumn(), saveFilePath);
          logger.error(e.getStackTrace());
        }
      });
    } catch (IOException e) {
      logger.fatal("db={},save_file_path={},msg=Failed to save file",
        planModel.getDbName(), saveFilePath);
      logger.fatal(e.getStackTrace());
    }
  }
}
