package com.github.DarkninjaD.Subscribers;

import com.github.DarkninjaD.Handlers.MovieHandler;

/**
 * DataLogger
 */
public class MovieLogger {

  public MovieLogger(MovieHandler handler) {
    handler
      .getProcessedFlow()
      .subscribe(
        item -> logData(item),
        error -> logError(error),
        () -> logCompletion()
      );
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
