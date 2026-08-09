package com.github.DarkninjaD.Adapters;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.DarkninjaD.Interfaces.IMovieProvider;
import com.github.DarkninjaD.Models.MovieDTO;
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
    return Flux.using(
      //Wrap in a lambda so it doesn't execute immediately
      () -> openSource(),
      // A Method
      publish(),
      // Method Reference
      this::onClose
    );
  }

  private Function<
    ? super JsonParser,
    ? extends Publisher<? extends MovieDTO>
  > publish() {
    return parser -> {
      try {
        MappingIterator<InnerJsonMovieRecord> iterator = mapper
          .readerFor(InnerJsonMovieRecord.class)
          .readValues(parser);

        return Flux.fromIterable(() -> iterator).map(parsed -> {
          return new MovieDTO(
            parsed.originalTitle(),
            parsed.runtimeMinutes(),
            Year.of(parsed.startYear()),
            "JSON"
          );
        });
      } catch (IOException e) {
        return Flux.error(new RuntimeException("Failed to stream JSON", e));
      }
    };
  }

  private JsonParser openSource() throws IOException {
    JsonParser parser = mapper.getFactory().createParser(filePath);

    if (parser.nextToken() != JsonToken.START_ARRAY) {
      throw new IllegalStateException("Json file needs to start with [");
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
