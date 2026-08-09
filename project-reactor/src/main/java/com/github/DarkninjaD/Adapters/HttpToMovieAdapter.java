package com.github.DarkninjaD.Adapters;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.DarkninjaD.Interfaces.IMovieProvider;
import com.github.DarkninjaD.Models.MovieDTO;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Year;
import java.util.concurrent.TimeoutException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

public class HttpToMovieAdapter implements IMovieProvider {

  private record InnerJsonMovieRecord(
    String originalTitle,
    int runtimeMinutes,
    int startYear, // Assuming int here, mapping to Year below
    String[] genres
  ) {}

  private final String apiUrl;
  private final HttpClient httpClient;
  private final ObjectMapper mapper;

  public HttpToMovieAdapter(String apiUrl) {
    this.apiUrl = apiUrl;
    this.mapper = new ObjectMapper();
    this.httpClient = HttpClient.newBuilder()
      .connectTimeout(Duration.ofSeconds(5))
      .build();
  }

  @Override
  public Flux<MovieDTO> getFlow() {
    HttpRequest request = HttpRequest.newBuilder()
      .uri(URI.create(apiUrl))
      .GET()
      .build();

    // 1. Request the body as an InputStream instead of a String
    return Mono.fromFuture(() ->
      httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofInputStream())
    )
      .flatMapMany(response -> {
        if (response.statusCode() >= 200 && response.statusCode() < 300) {
          // 2. Pass the InputStream directly into our streaming parser
          return streamFromInputStream(response.body());
        } else {
          return Flux.error(
            new RuntimeException(
              "HTTP GET failed with status: " + response.statusCode()
            )
          );
        }
      })
      // ===== Resilience & Fault Tolerance =====
      .timeout(Duration.ofSeconds(3))
      .retryWhen(
        Retry.backoff(3, Duration.ofSeconds(2)).filter(
          t -> t instanceof TimeoutException || t.getMessage().contains("500")
        )
      )
      .onErrorResume(e -> {
        System.err.println(
          "HTTP Source failed after retries: " + e.getMessage()
        );
        return Flux.empty();
      });
  }

  private Flux<MovieDTO> streamFromInputStream(InputStream inputStream) {
    // Reusing Flux.using() to ensure the network stream and parser are safely closed
    return Flux.using(
      () -> openSource(inputStream),
      parser -> {
        try {
          MappingIterator<InnerJsonMovieRecord> iterator = mapper
            .readerFor(InnerJsonMovieRecord.class)
            .readValues(parser);

          return Flux.fromIterable(() -> iterator).map(parsed -> {
            return new MovieDTO(
              parsed.originalTitle(),
              parsed.runtimeMinutes(),
              Year.of(parsed.startYear()),
              "HTTP_API"
            );
          });
        } catch (IOException e) {
          return Flux.error(
            new RuntimeException("Failed to stream HTTP JSON array", e)
          );
        }
      },
      this::onClose
    );
  }

  private JsonParser openSource(InputStream inputStream)
    throws IOException, JsonParseException {
    JsonParser parser = mapper.getFactory().createParser(inputStream);

    if (parser.nextToken() != JsonToken.START_ARRAY) {
      throw new IllegalStateException("Json steam needs to start with [");
    }
    parser.nextToken();
    return parser;
  }

  private void onClose(JsonParser parser) {
    try {
      parser.close();
    } catch (IOException e) {}
  }
}
