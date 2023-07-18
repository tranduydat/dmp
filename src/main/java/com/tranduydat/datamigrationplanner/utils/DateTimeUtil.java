package com.tranduydat.datamigrationplanner.utils;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Utility class for date and time operations.
 *
 * @author Dat Tran (dattd6)
 * @version 0.1
 * @since 2023/07/13
 */
public class DateTimeUtil {
  private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
  /**
   * Retrieves the current time as a formatted string.
   *
   * @return The current time in the format "yyyy/MM/dd HH:mm:ss".
   */
  public static String getCurrentTime() {
    LocalDateTime currentTime = LocalDateTime.now();
    return currentTime.format(FORMATTER);
  }
}
