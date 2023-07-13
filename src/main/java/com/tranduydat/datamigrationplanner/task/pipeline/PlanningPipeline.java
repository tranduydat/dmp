package com.tranduydat.datamigrationplanner.task.pipeline;

import com.tranduydat.datamigrationplanner.config.Config;
import com.tranduydat.datamigrationplanner.config.Constant;
import com.tranduydat.datamigrationplanner.model.DbModel;
import com.tranduydat.datamigrationplanner.model.PlanModel;
import com.tranduydat.datamigrationplanner.task.process.DbProcess;
import com.tranduydat.datamigrationplanner.task.process.MakingPlanProcess;
import com.tranduydat.datamigrationplanner.task.process.SavingPlanProcess;
import com.tranduydat.datamigrationplanner.task.utils.DateTimeUtil;
import lombok.*;
import org.apache.logging.log4j.LogManager;

import java.sql.SQLException;

/**
 * @author Dat Tran (dattd6)
 * @version 0.1
 * @since 2023/07/13
 */

@NoArgsConstructor
public class PlanningPipeline implements Runnable {
  private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger(PlanningPipeline.class);
  private Config config = Config.getInstance();

  @Override
  public void run() {
    logger.info("\n" + Constant.ASCII_LOGO);

    String jobInfoMsg = "\nStart at: " + DateTimeUtil.getCurrentTime()
      + "\n- Target host: " + config.getMssqlServerHost()
      + "\n- Target port: " + config.getMssqlServerPort()
      + "\n- Target database: " + config.getMssqlServerDb()
      + "\n- Max connections size: " + config.getCpMaxSize()
      + "\n- Min idle connection: " + config.getCpMinIdle()
      + "\n";
    logger.info(Constant.LINEBAR);

    try {
      long startTime, endTime;

      startTime = System.currentTimeMillis();
      DbProcess dbPTask = new DbProcess();
      DbModel dbModel = dbPTask.get(config.getMssqlServerDb());
      endTime = System.currentTimeMillis();
      logger.info("DbProcess time: " + (endTime - startTime) + " ms\n" + Constant.LINEBAR);

      startTime = System.currentTimeMillis();
      MakingPlanProcess makingPlanPTask = new MakingPlanProcess();
      PlanModel planModel = makingPlanPTask.make(dbModel);
      endTime = System.currentTimeMillis();
      logger.info("MakingPlanProcess time: " + (endTime - startTime) + " ms\n" + Constant.LINEBAR);

      startTime = System.currentTimeMillis();
      SavingPlanProcess savingPlanPTask = new SavingPlanProcess();
      savingPlanPTask.save(config.getSavePath(), planModel, Constant.DELIMITER);
      endTime = System.currentTimeMillis();
      logger.info("SavingPlanProcess time: " + (endTime - startTime) + " ms\n" + Constant.LINEBAR);
    } catch (SQLException e) {
      logger.fatal("Failed to get MS SQL Server connection");
    }
  }
}
