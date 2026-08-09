package com.github.DarkninjaD.Adapters;

import com.github.DarkninjaD.Interfaces.IMovieProvider;
import com.github.DarkninjaD.Models.MovieDTO;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.Year;
import reactor.core.publisher.Flux;

public class CsvToMovieAdapter implements IMovieProvider {

  private final String filePath;

  public CsvToMovieAdapter(String filepath) {
    this.filePath = filepath;
  }

  @Override
  public Flux<MovieDTO> getFlow() {
    return Flux.using(
      () -> getClass().getResourceAsStream(filePath),
      inputStream -> {
        BufferedReader reader = new BufferedReader(
          new InputStreamReader(inputStream)
        );

        return Flux.fromStream(reader.lines());
      },
      inputStream -> {
        try {
          inputStream.close();
        } catch (IOException e) {}
      }
    )
      .skip(1)
      .map(line -> {
        String[] parts = line.split(",");
        return new MovieDTO(
          parts[0].trim(),
          Integer.parseInt(parts[1].trim()),
          Year.parse(parts[2].trim()),
          //parts[3].split(" "),
          "CSV"
        );
      });
  }
}
