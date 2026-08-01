package com.github.DarkninjaD.Interfaces;

import com.github.DarkninjaD.Models.DataDTO;

import reactor.core.publisher.Flux;

/**
 * IDataProvider -
 * The Interface enforce that each class will have
 * The method signature and the given return type.
 *
 * @methods getFlow()
 * @return a {@link Flux flow} of our {@link DataDTO}
 */
public interface IDataProvider {
  String id = "I'm a interface";

  Flux<DataDTO> getFlow();
}
