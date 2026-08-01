package com.github.DarkninjaD.Subscribers;

import com.github.DarkninjaD.Handlers.DataHandler;

/**
 * DataLogger
 */
public class DataLogger {

  public DataLogger(DataHandler handler) {
    handler.getProcessedFlow().subscribe(
        item -> logData(item),
        error -> logError(error),
        () -> logCompletion());
  }

  private void logData(String item) {
    System.out.println("Logger Output: " + item);
  }

  private void logError(Throwable error) {
    System.out.println("Logger Output [ERROR]: " + error.getMessage());
  }

  private void logCompletion() {
    System.out.println("Logger Output: All data has been outputted.");
  }
}
