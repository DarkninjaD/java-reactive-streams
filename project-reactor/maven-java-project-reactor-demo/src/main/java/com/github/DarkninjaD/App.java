package com.github.DarkninjaD;

import com.github.DarkninjaD.Adapters.CsvToMovieAdapter;
import com.github.DarkninjaD.Adapters.JsonToMovieAdapter;
import com.github.DarkninjaD.Handlers.MovieHandler;
import com.github.DarkninjaD.Subscribers.MovieLogger;
import java.net.URI;
import java.util.List;

/**
 * Hello world!
 */
public class App {

  public static void main(String[] args) throws InterruptedException {
    CsvToMovieAdapter csvAdapter = new CsvToMovieAdapter(
      "/sample_movies_1993.csv"
    );
    JsonToMovieAdapter jsonAdapter = new JsonToMovieAdapter(
      "/sample_movies_1993.json"
    );

    MovieHandler handler = new MovieHandler(List.of(csvAdapter, jsonAdapter));

    MovieLogger log = new MovieLogger(handler);

    Thread.sleep(1000);
  }
}
