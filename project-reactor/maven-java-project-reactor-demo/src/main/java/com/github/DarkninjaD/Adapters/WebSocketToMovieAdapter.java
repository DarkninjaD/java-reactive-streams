package com.github.DarkninjaD.Adapters;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.DarkninjaD.Interfaces.IMovieProvider;
import com.github.DarkninjaD.Models.MovieDTO;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.time.Duration;
import java.time.Year;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
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
    // 1. Flux.create safely bridges callbacks and handles multiple subscribers/retries
    return Flux.<MovieDTO>create(sink -> {
      // Initialize the listener with the FluxSink
      MovieWebSocketListener listener = new MovieWebSocketListener(
        sink,
        mapper
      );

      // Connect to the WebSocket
      CompletableFuture<WebSocket> wsFuture = httpClient
        .newWebSocketBuilder()
        .buildAsync(URI.create(wsUrl), listener);

      // 2. Cleanup: If the downstream Flux cancels (e.g. .take(5)), safely close the WebSocket
      sink.onDispose(() -> {
        wsFuture.thenAccept(ws -> {
          if (ws != null) {
            ws.sendClose(
              WebSocket.NORMAL_CLOSURE,
              "Client cancelling subscription"
            );
          }
        });
      });
    })
      // 3. Resilience: If the WS drops, this will cleanly re-invoke Flux.create() and reconnect!
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

    private final FluxSink<MovieDTO> sink;
    private final ObjectMapper mapper;
    private StringBuilder messageBuffer;

    public MovieWebSocketListener(
      FluxSink<MovieDTO> sink,
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
            sink.next(dto);
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
      sink.complete();
      return null;
    }

    @Override
    public void onError(WebSocket webSocket, Throwable error) {
      // Push network errors down the reactive pipeline to trigger our retryWhen() logic
      System.err.println("[WS] Network ERROR detected: " + error.getMessage());
      sink.error(error);
    }
  }
}
