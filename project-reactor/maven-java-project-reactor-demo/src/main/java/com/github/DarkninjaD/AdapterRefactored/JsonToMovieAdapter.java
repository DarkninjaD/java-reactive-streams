package com.github.DarkninjaD.AdapterRefactored;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.DarkninjaD.Interfaces.IMovieProvider;
import com.github.DarkninjaD.Models.MovieDTO;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.time.Year;
import java.util.function.Function;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

public class JsonToMovieAdapter implements IMovieProvider {

  /**
   * InnerJsonMovieRecord
   */
  private record InnerJsonMovieRecord(
    String originalTitle,
    int runtimeMinutes,
    int startYear,
    String[] genres
  ) {}

  private final URL filePath;
  private final ObjectMapper mapper;

  public JsonToMovieAdapter(String fileURI) {
    this.filePath = getClass().getResource(fileURI);
    this.mapper = new ObjectMapper();
  }

  @Override
  public Flux<MovieDTO> getFlow() {
    return Flux.using(this::openSource, this::publish, this::onClose);
  }

  private JsonParser openSource() throws IOException {
    JsonParser parser = mapper.getFactory().createParser(filePath);

    if (parser.nextToken() != JsonToken.START_ARRAY) {
      throw new IllegalStateException("Json file needs to start with [");
    }
    parser.nextToken();
    return parser;
  }

  // 2. The Stream Generator (Function<JsonParser, Publisher<MovieDTO>>)
  // Takes the resource directly. Cannot throw checked exceptions.
  private Flux<MovieDTO> publish(JsonParser parser) {
    try {
      MappingIterator<InnerJsonMovieRecord> iterator = mapper
        .readerFor(InnerJsonMovieRecord.class)
        .readValues(parser);

      return Flux.fromIterable(() -> iterator).map(parsed ->
        new MovieDTO(
          parsed.originalTitle(),
          parsed.runtimeMinutes(),
          Year.of(parsed.startYear()),
          "JSON"
        )
      );
    } catch (IOException e) {
      // If Jackson fails here, push the error into the Reactor pipeline safely
      return Flux.error(new RuntimeException("Failed to stream JSON", e));
    }
  }

  // 3. The Cleanup (Consumer<JsonParser>)
  // Cannot throw checked exceptions. Fails silently or logs if closing fails.
  private void onClose(JsonParser parser) {
    if (parser != null) {
      try {
        parser.close();
      } catch (IOException e) {
        System.err.println(
          "[WARN] Failed to gracefully close JsonParser: " + e.getMessage()
        );
      }
    }
  }
}
