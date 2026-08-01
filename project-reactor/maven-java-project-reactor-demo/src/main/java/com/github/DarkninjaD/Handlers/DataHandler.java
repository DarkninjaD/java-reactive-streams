package com.github.DarkninjaD.Handlers;

import java.util.List;

import com.github.DarkninjaD.Interfaces.IDataProvider;
import com.github.DarkninjaD.Models.DataDTO;

import reactor.core.publisher.Flux;

public class DataHandler {
  private final Flux<String> downstreamFlow;

  public DataHandler(List<IDataProvider> providers) {

    // Just to prove that This is just interface we are looking at.
    // see how we are able to print Id even thought both Adapters don't
    // have that value.
    // System.out.println(providers.get(0).id);

    // Here we take our flow from each adapter
    List<Flux<DataDTO>> allFlows = providers.stream()
        .map(IDataProvider::getFlow)
        .toList();

    // so we can feed it's output our 
    this.downstreamFlow = Flux.merge(allFlows)
        .map(dto -> processDto(dto));
  }

  // Where we do work all the work on the unified data.
  // Also know as a Data Transfer Object.
  private String processDto(DataDTO dto) {
    return String.format("[PROCESSED] Movie Title: %s | Run time: %.0f mins | short description from: %s",
        dto.title(), dto.runtime(), dto.description());
  }

  public Flux<String> getProcessedFlow() {
    return downstreamFlow;
  }
}
