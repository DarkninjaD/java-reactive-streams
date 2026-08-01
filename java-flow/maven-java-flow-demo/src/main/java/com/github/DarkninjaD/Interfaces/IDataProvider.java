package com.github.DarkninjaD.Interfaces;

import java.util.concurrent.Flow;

public interface IDataProvider {

  Flow.Publisher<String> getPublisher();
}