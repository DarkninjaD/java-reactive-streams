package com.github.DarkninjaD.Adapters;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.DarkninjaD.Interfaces.IMovieProvider;
import com.github.DarkninjaD.Models.MovieDTO;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.time.Duration;
import java.time.Year;
import java.util.concurrent.CompletionStage;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;
import reactor.util.retry.Retry;

public class WebSocketToMovieAdapter implements IMovieProvider {

  // Formatted inline
  private record InnerJsonMovieRecord(
    String originalTitle,
    int runtimeMinutes,
    int startYear,
    String[] genres
  ) {}

  private final String wsUrl;
  private final ObjectMapper mapper;
  private final HttpClient httpClient;

  public WebSocketToMovieAdapter(String wsUrl) {
    this.wsUrl = wsUrl;
    this.mapper = new ObjectMapper();
    this.httpClient = HttpClient.newHttpClient();
  }

  @Override
  public Flux<MovieDTO> getFlow() {
    // 1. Create a unicast Sink to act as the bridge between the WebSocket callbacks and our Flux
    Sinks.Many<MovieDTO> sink = Sinks.many().unicast().onBackpressureBuffer();

    // 2. Defer connection until something actually subscribes to the pipeline
    return Flux.defer(() -> {
      httpClient
        .newWebSocketBuilder()
        .buildAsync(
          URI.create(wsUrl),
          new MovieWebSocketListener(sink, mapper)
        );

      // Emit the stream of DTOs from the sink
      return sink.asFlux();
    })
      // 3. Add resilience: if the WebSocket drops, wait 2 seconds and try to reconnect
      .retryWhen(Retry.backoff(3, Duration.ofSeconds(2)))
      .onErrorResume(e -> {
        System.err.println(
          "WebSocket disconnected permanently: " + e.getMessage()
        );
        return Flux.empty();
      });
  }

  // ===== The Callback Listener =====
  private static class MovieWebSocketListener implements WebSocket.Listener {

    private final Sinks.Many<MovieDTO> sink;
    private final ObjectMapper mapper;
    private StringBuilder messageBuffer;

    public MovieWebSocketListener(
      Sinks.Many<MovieDTO> sink,
      ObjectMapper mapper
    ) {
      this.sink = sink;
      this.mapper = mapper;
      this.messageBuffer = new StringBuilder();
    }

    @Override
    public void onOpen(WebSocket webSocket) {
      WebSocket.Listener.super.onOpen(webSocket);
      // Tell the server we are ready to receive the first message
      webSocket.request(1);
    }

    @Override
    public CompletionStage<?> onText(
      WebSocket webSocket,
      CharSequence data,
      boolean last
    ) {
      // Append the incoming chunk to our buffer
      messageBuffer.append(data);

      // Wait until we have a full JSON from the stream before parsing and publishing
      if (last) {
        String fullJson = messageBuffer.toString();
        try {
          // Parse the matching JSON array block
          if (!fullJson.isBlank()) {
            InnerJsonMovieRecord parser = mapper.readValue(
              fullJson,
              InnerJsonMovieRecord.class
            );

            MovieDTO dto = new MovieDTO(
              parser.originalTitle(),
              parser.runtimeMinutes(),
              Year.of(parser.startYear()),
              "WEBSOCKET_API"
            );
            // Safely push the newly parsed objects into the Reactor pipeline
            sink.tryEmitNext(dto);
          }
        } catch (Exception e) {
          System.err.println(
            "[WS WARN] Skipping malformed message: " + fullJson.trim()
          );
          // sink.tryEmitError(
          //   new RuntimeException(
          //     "Failed to parse WebSocket JSON array block",
          //     e
          //   )
          // );
        } finally {
          // Reset the buffer for the next incoming array block
          messageBuffer = new StringBuilder();
        }

        webSocket.request(1);
        return null;
      }

      // Tell the server we are ready for the next chunk/message
      webSocket.request(1);
      return null;
    }

    @Override
    public CompletionStage<?> onClose(
      WebSocket webSocket,
      int statusCode,
      String reason
    ) {
      // Gracefully close the Reactor stream when the WebSocket shuts down
      sink.tryEmitComplete();
      return null;
    }

    @Override
    public void onError(WebSocket webSocket, Throwable error) {
      // Push network errors down the reactive pipeline to trigger our retryWhen() logic
      sink.tryEmitError(error);
    }
  }
}
