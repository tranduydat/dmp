package com.tranduydat.datamigrationplanner;

import com.tranduydat.datamigrationplanner.config.Config;
import com.tranduydat.datamigrationplanner.task.pipeline.PlanningPipeline;
import com.tranduydat.datamigrationplanner.task.process.MakingPlanProcess;
import org.apache.commons.cli.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Data Migration Planner (DMP)
 * Just a hobby project in my free time!
 *
 * @author Dat Tran (dattd6)
 * @version 1
 * @since 2023/07/13
 */
public class Main {
  private static final Logger logger = LogManager.getLogger(Main.class);

  public static void main(String[] args) {
    // Check if DS_PASSWORD environment variable is set
    String password = System.getenv("DMP_DS_PASSWORD");
    if (password == null) {
      System.out.println("DMP_DS_PASSWORD environment variable is missing. Exiting the application.");
      System.exit(1);
    }

    // Initialize Log4j 2 with the configuration file
    System.setProperty("log4j.configurationFile", "log4j2.properties");

    Options options = new Options();
    options.addOption(Option.builder("u").longOpt("ds_username").hasArg().required().desc("The username for the datasource").build());
    options.addOption(Option.builder("h").longOpt("ds_host").hasArg().required().desc("The hostname for the datasource").build());
    options.addOption(Option.builder("p").longOpt("ds_port").hasArg().required().desc("The port number for the datasource").build());
    options.addOption(Option.builder("d").longOpt("ds_db").hasArg().required().desc("The name of the database for the datasource").build());
    options.addOption(Option.builder("m").longOpt("cp_max_size").hasArg().desc("The maximum size of the connection pool").build());
    options.addOption(Option.builder("i").longOpt("cp_min_idle").hasArg().desc("The minimum number of idle connections in the pool").build());
    options.addOption(Option.builder("s").longOpt("save_path").hasArg().required().desc("The save path").build());

    CommandLineParser parser = new DefaultParser();

    try {
      CommandLine cmd = parser.parse(options, args);

      String username = cmd.getOptionValue("u");
      String host = cmd.getOptionValue("h");
      int port = Integer.parseInt(cmd.getOptionValue("p"));
      String db = cmd.getOptionValue("d");
      int maxConnections = Integer.parseInt(cmd.getOptionValue("m", "5"));  // Default to 5 if not provided
      int minIdleConnections = Integer.parseInt(cmd.getOptionValue("i", "1"));  // Default to 1 if not provided
      String savePath = cmd.getOptionValue("s");

      System.out.println(host + "\n\n");

      Config.getInstance().setMssqlServerHost(host);
      Config.getInstance().setMssqlServerPort(port);
      Config.getInstance().setMssqlServerDb(db);
      Config.getInstance().setMssqlServerUsername(username);
      Config.getInstance().setMssqlServerPassword(password);
      Config.getInstance().setCpMaxSize(maxConnections);
      Config.getInstance().setCpMinIdle(minIdleConnections);
      Config.getInstance().setSavePath(savePath);

      PlanningPipeline planningPipeline = new PlanningPipeline();
      // Run pipeline
      planningPipeline.run();

    } catch (ParseException e) {
      logger.fatal("Error parsing command-line arguments: " + e.getMessage(), e);
    }
  }
}
