package com.github.DarkninjaD.Adapters;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.DarkninjaD.Interfaces.IMovieProvider;
import com.github.DarkninjaD.Models.MovieDTO;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Year;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class HttpRepeatToMovieAdapter implements IMovieProvider {

  private final URI url;
  private final ObjectMapper mapper;
  private final HttpClient httpClient;
  private final long DurationInSeconds;

  private record InnerJsonMovieRecord(
    String originalTitle,
    int runtimeMinutes,
    int startYear,
    String[] genres
  ) {}

  public HttpRepeatToMovieAdapter(String urlString, long durationInSeconds) {
    this.url = URI.create(urlString);
    this.mapper = new ObjectMapper();
    this.httpClient = HttpClient.newBuilder()
      .connectTimeout(Duration.ofSeconds(5))
      .build();
    this.DurationInSeconds = durationInSeconds;
  }

  @Override
  public Flux<MovieDTO> getFlow() {
    HttpRequest request = HttpRequest.newBuilder().uri(url).GET().build();

    return Flux.interval(Duration.ofSeconds(DurationInSeconds)).flatMap(tick ->
      Mono.fromFuture(() ->
        httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
      ).flatMap(response -> {
        if (response.statusCode() >= 200 && response.statusCode() < 300) {
          try {
            InnerJsonMovieRecord formattedData = mapper.readValue(
              response.body(),
              InnerJsonMovieRecord.class
            );

            MovieDTO movieDTO = new MovieDTO(
              formattedData.originalTitle(),
              formattedData.runtimeMinutes(),
              Year.of(formattedData.startYear()),
              "HTTP_REPEAT"
            );

            return Mono.just(movieDTO);
          } catch (Exception e) {
            return Mono.error(new RuntimeException("dang"));
          }
        } else {
          return Mono.error(
            new RuntimeException(
              "HTTP GET failed with status: " + response.statusCode()
            )
          );
        }
      })
    );
  }
}
