package com.github.DarkninjaD.Adapters;

import java.time.Duration;

import com.github.DarkninjaD.Interfaces.IDataProvider;
import com.github.DarkninjaD.Models.DataDTO;

import reactor.core.publisher.Flux;

public class CsvDataAdapter implements IDataProvider {

  @Override
  public Flux<DataDTO> getFlow() {
    return Flux.interval(Duration.ofMillis(500))
        .take(3)
        .map(tick -> {
          String fakeCsvName = "CSV: Test " + (100 + tick);
          String fakeCsvDescription = "much to do about " + (tick);
          double fakeCsvAmount = (0 + tick);

          return new DataDTO(fakeCsvName, fakeCsvDescription, fakeCsvAmount);

        });
  }
}
