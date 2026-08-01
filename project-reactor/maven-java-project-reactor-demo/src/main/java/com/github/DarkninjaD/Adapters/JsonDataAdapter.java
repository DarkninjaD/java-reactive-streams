package com.github.DarkninjaD.Adapters;

import java.time.Duration;

import com.github.DarkninjaD.Interfaces.IDataProvider;
import com.github.DarkninjaD.Models.DataDTO;

import reactor.core.publisher.Flux;

public class JsonDataAdapter implements IDataProvider {

  @Override
  public Flux<DataDTO> getFlow() {
    return Flux.interval(Duration.ofMillis(300))
        .take(3)
        .map(tick -> {
          String fakeJsonName = "Json: Test " + (100 + tick);
          String fakeJsonDescription = "much to do about " + (tick);
          double fakeJsonAmount = (0 + tick);

          return new DataDTO(fakeJsonName, fakeJsonDescription, fakeJsonAmount);

        });
  }

}
