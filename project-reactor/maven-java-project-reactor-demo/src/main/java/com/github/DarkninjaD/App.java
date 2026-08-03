package com.github.DarkninjaD;

import com.github.DarkninjaD.Adapters.CsvToMovieAdapter;
import com.github.DarkninjaD.Adapters.HttpToMovieAdapter;
import com.github.DarkninjaD.Adapters.JsonToMovieAdapter;
import com.github.DarkninjaD.Adapters.WebSocketToMovieAdapter;
import com.github.DarkninjaD.Handlers.MovieHandler;
import com.github.DarkninjaD.Handlers.MovieHandlerSink;
import com.github.DarkninjaD.Subscribers.MovieLogger;
import java.util.List;
import reactor.core.publisher.Flux;

public class App {

  private static final boolean toggle = false;

  public static void main(String[] args) throws InterruptedException {
    CsvToMovieAdapter csvAdapter = new CsvToMovieAdapter(
      "/sample_movies_1993.csv"
    );
    JsonToMovieAdapter jsonAdapter = new JsonToMovieAdapter(
      "/sample_movies_1993.json"
    );
    HttpToMovieAdapter httpAdapter = new HttpToMovieAdapter(
      "http://localhost:8080/movies"
    );
    WebSocketToMovieAdapter wsAdapter = new WebSocketToMovieAdapter(
      "ws://localhost:8080/ws"
    );

    if (toggle) {
      MovieHandler handler = new MovieHandler(
        List.of(csvAdapter, jsonAdapter, httpAdapter)
      );

      Flux<String> fluxCapacitor = handler.getProcessedFlow();
      MovieLogger log = new MovieLogger(fluxCapacitor);
    } else {
      MovieHandlerSink sinkHandler = new MovieHandlerSink();

      Flux<String> fluxCapacitor = sinkHandler.getAggregatedStream();

      MovieLogger log = new MovieLogger(fluxCapacitor);

      sinkHandler.registerProvider(jsonAdapter);
      sinkHandler.registerProvider(csvAdapter);
      sinkHandler.registerProvider(httpAdapter);
      sinkHandler.registerProvider(wsAdapter);
    }

    Thread.sleep(1000);
  }
}
