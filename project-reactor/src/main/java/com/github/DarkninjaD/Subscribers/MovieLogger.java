package com.github.DarkninjaD.Subscribers;

import reactor.core.publisher.Flux;

/**
 * DataLogger
 */
public class MovieLogger {

  public MovieLogger(Flux<String> handler) {
    handler.subscribe(
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
