package com.tranduydat.datamigrationplanner.pipeline;

import com.tranduydat.datamigrationplanner.config.Config;
import com.tranduydat.datamigrationplanner.config.Constant;
import com.tranduydat.datamigrationplanner.model.DbModel;
import com.tranduydat.datamigrationplanner.model.PlanModel;
import com.tranduydat.datamigrationplanner.task.DbReader;
import com.tranduydat.datamigrationplanner.task.MakingPlanProcessor;
import com.tranduydat.datamigrationplanner.task.PlanWriter;
import com.tranduydat.datamigrationplanner.utils.DateTimeUtil;
import lombok.NoArgsConstructor;
import org.apache.logging.log4j.LogManager;

import java.sql.SQLException;

/**
 * The pipeline for generating and saving a plan.
 *
 * @author Dat Tran (dattd6)
 * @version 0.1
 * @since 2023/07/13
 */
@NoArgsConstructor
public class MakingPlanPipeline implements Runnable {
  private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger(MakingPlanPipeline.class);
  private final Config config = Config.getInstance();

  /**
   * Generates the plan model based on the database model.
   *
   * @param dbModel The database model.
   * @return The generated plan model.
   */
  private static PlanModel make(DbModel dbModel) {
    long startTime, endTime;
    startTime = System.currentTimeMillis();
    MakingPlanProcessor processor = new MakingPlanProcessor();
    PlanModel planModel = processor.makeAndGet(dbModel);
    endTime = System.currentTimeMillis();
    logger.info("MakingPlanProcess time: " + (endTime - startTime) + " ms\n");
    return planModel;
  }

  @Override
  public void run() {
    // Print out DMP logo and job info
    showJobInfo();

    try {
      // Step 1: Database Reading
      DbModel dbModel = read();

      // Step 2: Plan Processing
      PlanModel planModel = make(dbModel);

      // Step 3: Plan Saving
      write(planModel);
    } catch (SQLException e) {
      logger.fatal("Failed to get MS SQL Server connection");
    }
  }

  /**
   * Writes the plan model to a file.
   *
   * @param planModel The plan model to be saved.
   */
  private void write(PlanModel planModel) {
    long startTime, endTime;
    startTime = System.currentTimeMillis();
    PlanWriter writer = new PlanWriter();
    // Write plan to a file (read from args)
    writer.write(config.getSavePath(), planModel, Constant.DELIMITER);
    endTime = System.currentTimeMillis();
    logger.info("SavingPlanProcess time: " + (endTime - startTime) + " ms\n");
  }

  /**
   * Reads the database model from the database.
   *
   * @return The retrieved database model.
   * @throws SQLException if there is an error in getting the MS SQL Server connection.
   */
  private DbModel read() throws SQLException {
    long startTime, endTime;
    startTime = System.currentTimeMillis();
    DbReader reader = new DbReader();
    // Get all tables, and columns of each table in a database (read from args)
    DbModel dbModel = reader.get(config.getMssqlServerDb());
    endTime = System.currentTimeMillis();
    logger.info("DbProcess time: " + (endTime - startTime) + " ms\n");
    return dbModel;
  }

  /**
   * Prints the ASCII logo and job information.
   */
  private void showJobInfo() {
    logger.info("\n" + Constant.ASCII_LOGO);

    String jobInfoMsg = "\nStart at: " + DateTimeUtil.getCurrentTime()
      + "\n- Target host: " + config.getMssqlServerHost()
      + "\n- Target port: " + config.getMssqlServerPort()
      + "\n- Target database: " + config.getMssqlServerDb()
      + "\n- Max connections size: " + config.getCpMaxSize()
      + "\n- Min idle connection: " + config.getCpMinIdle()
      + "\n";
    logger.info(jobInfoMsg);
  }
}
