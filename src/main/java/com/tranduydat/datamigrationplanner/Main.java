package com.tranduydat.datamigrationplanner;

import com.tranduydat.datamigrationplanner.config.Config;
import com.tranduydat.datamigrationplanner.pipeline.MakingPlanPipeline;
import org.apache.commons.cli.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Data Migration Planner (DMP)
 * Just a hobby project in my free time!
 * This class serves as the entry point for the application.
 *
 * @author Dat Tran (dattd6)
 * @version 1
 * @since 2023/07/13
 */
public class Main {
  private static final Logger logger = LogManager.getLogger(Main.class);

  public static void main(String[] args) {
    // Check if DS_PASSWORD environment variable is set
    // If it's not, then exit
    String password = System.getenv("DMP_DS_PASSWORD");
    if (password == null) {
      logger.fatal("DMP_DS_PASSWORD environment variable is missing. Exiting the application.");
      System.exit(1);
    }

    // Initialize Log4j 2 with the configuration file
    System.setProperty("log4j.configurationFile", "log4j2.properties");

    // Parse command-line arguments and retrieve values
    CommandLine cmd = parseCommandLineArgs(args);

    // Set configuration values
    setConfigValues(cmd, password);

    // Run the making plan pipeline
    runMakingPlanPipeline();
  }

  /**
   * Parses the command-line arguments and returns the parsed CommandLine object.
   *
   * @param args The command-line arguments passed to the application.
   * @return The parsed CommandLine object.
   */
  private static CommandLine parseCommandLineArgs(String[] args) {
    Options options = createCommandLineOptions();

    CommandLineParser parser = new DefaultParser();

    try {
      return parser.parse(options, args);
    } catch (ParseException e) {
      logger.fatal("Error parsing command-line arguments: " + e.getMessage(), e);
      System.exit(1);
    }

    return null;
  }

  /**
   * Creates the command-line options using the Apache Commons CLI library and returns the Options object.
   *
   * @return The Options object containing the defined command-line options.
   */
  private static Options createCommandLineOptions() {
    Options options = new Options();
    options.addOption(Option.builder("u")
      .longOpt("ds_username").hasArg().required()
      .desc("The username for the datasource")
      .build());
    options.addOption(Option.builder("h")
      .longOpt("ds_host").hasArg().required()
      .desc("The hostname for the datasource")
      .build());
    options.addOption(Option.builder("p")
      .longOpt("ds_port").hasArg().required()
      .desc("The port number for the datasource")
      .build());
    options.addOption(Option.builder("d")
      .longOpt("ds_db").hasArg().required()
      .desc("The name of the database for the datasource")
      .build());
    options.addOption(Option.builder("m")
      .longOpt("cp_max_size").hasArg()
      .desc("The maximum size of the connection pool")
      .build());
    options.addOption(Option.builder("i")
      .longOpt("cp_min_idle").hasArg()
      .desc("The minimum number of idle connections in the pool")
      .build());
    options.addOption(Option.builder("s")
      .longOpt("save_path").hasArg().required()
      .desc("The save path")
      .build());

    return options;
  }

  /**
   * Sets the configuration values based on the parsed CommandLine object and the provided password.
   *
   * @param cmd      The parsed CommandLine object.
   * @param password The password retrieved from the environment variable.
   */
  private static void setConfigValues(CommandLine cmd, String password) {
    String username = cmd.getOptionValue("u");
    String host = cmd.getOptionValue("h");
    int port = Integer.parseInt(cmd.getOptionValue("p"));
    String db = cmd.getOptionValue("d");
    int maxConnections = Integer.parseInt(cmd.getOptionValue("m", "5")); // Default to 5 if not provided
    int minIdleConnections = Integer.parseInt(cmd.getOptionValue("i", "1")); // Default to 1 if not provided
    String savePath = cmd.getOptionValue("s");

    Config config = Config.getInstance();
    config.setMssqlServerHost(host);
    config.setMssqlServerPort(port);
    config.setMssqlServerDb(db);
    config.setMssqlServerUsername(username);
    config.setMssqlServerPassword(password);
    config.setCpMaxSize(maxConnections);
    config.setCpMinIdle(minIdleConnections);
    config.setSavePath(savePath);
  }

  /**
   * Creates an instance of the MakingPlanPipeline class and runs the pipeline.
   */
  private static void runMakingPlanPipeline() {
    MakingPlanPipeline makingPlanPipeline = new MakingPlanPipeline();
    makingPlanPipeline.run();
  }
}
