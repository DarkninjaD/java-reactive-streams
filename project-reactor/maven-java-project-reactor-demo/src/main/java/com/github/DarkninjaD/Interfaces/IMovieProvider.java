package com.github.DarkninjaD.Interfaces;

import com.github.DarkninjaD.Models.MovieDTO;
import reactor.core.publisher.Flux;

/**
 * IDataProvider -
 * The Interface enforce that each class will have
 * The method signature and the given return type.
 *
 * @methods getFlow()
 * @return a {@link Flux flow} of our {@link MovieDTO}
 */
public interface IMovieProvider {
  String id = "I'm a interface";

  Flux<MovieDTO> getFlow();
}
