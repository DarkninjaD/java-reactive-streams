package com.github.DarkninjaD.Handlers;

import com.github.DarkninjaD.Interfaces.IMovieProvider;
import com.github.DarkninjaD.Models.MovieDTO;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

public class MovieHandlerSink {

  // The central event bus for all incoming movie data
  private final Sinks.Many<String> centralSink;

  public MovieHandlerSink() {
    // Multicast allows multiple downstream subscribers (like a logger and a database)
    // onBackpressureBuffer safely queues items if the downstream processor gets slow
    this.centralSink = Sinks.many().multicast().onBackpressureBuffer();
  }

  // ===== Dynamic Provider Registration =====
  public Disposable registerProvider(IMovieProvider provider) {
    // We take the provider's stream and pipe it directly into our central sink
    return provider
      .getFlow()
      .subscribe(
        dto -> centralSink.tryEmitNext(processDto(dto)),
        error ->
          System.err.println(
            "A provider encountered an error: " + error.getMessage()
          )
        // Note: We deliberately DO NOT call centralSink.tryEmitComplete() here.
        // If a finite source (like a CSV file) finishes, we want the central sink
        // to stay open so persistent sources (like WebSockets) can keep streaming.
      );
  }

  // Where we do work all the work on the unified data.
  // Also know as a Data Transfer Object.
  private String processDto(MovieDTO dto) {
    return String.format(
      "[PROCESSED] Movie Title: %s | Run time: %d mins | source: %s",
      dto.title(),
      dto.runtime(),
      dto.source()
    );
  }

  // ===== Downstream Output =====
  public Flux<String> getAggregatedStream() {
    // This is what your final logger or database adapter will subscribe to
    return centralSink.asFlux();
  }
}
