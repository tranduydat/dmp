package com.tranduydat.datamigrationplanner.utils;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class DateTimeUtil {
  private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");

  public static String getCurrentTime() {
    LocalDateTime currentTime = LocalDateTime.now();
    return currentTime.format(FORMATTER);
  }
}
