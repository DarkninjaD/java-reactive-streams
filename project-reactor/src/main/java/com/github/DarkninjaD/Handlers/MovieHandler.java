package com.github.DarkninjaD.Handlers;

import com.github.DarkninjaD.Interfaces.IMovieProvider;
import com.github.DarkninjaD.Models.MovieDTO;
import java.util.List;
import reactor.core.publisher.Flux;

/**
 * DataHandler This handler of the data source. Once it's given all the data source it's going to
 * handle. it provides a way to feed the data to a subscriber thought a method.
 *
 * @method getProcessedFlow()
 */
public class MovieHandler {

  private final Flux<String> downstreamFlow;

  public MovieHandler(List<IMovieProvider> providers) {
    // Just to prove that This is just interface we are looking at.
    // see how we are able to print Id even thought both Adapters don't
    // have that value.
    // System.out.println(providers.get(0).id);

    // Here we take our flow from each adapter
    List<Flux<MovieDTO>> allFlows = providers
      .stream()
      .map(IMovieProvider::getFlow)
      .toList();

    // so we can feed it's output our
    this.downstreamFlow = Flux.merge(allFlows).map(dto -> processDto(dto));
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

  public Flux<String> getProcessedFlow() {
    return downstreamFlow;
  }
}
