package com.github.DarkninjaD;

import java.util.List;

import com.github.DarkninjaD.Adapters.CsvDataAdapter;
import com.github.DarkninjaD.Adapters.JsonDataAdapter;
import com.github.DarkninjaD.Handlers.DataHandler;
import com.github.DarkninjaD.Subscribers.DataLogger;

/**
 * Hello world!
 */
public class App {
    public static void main(String[] args) throws InterruptedException {
        CsvDataAdapter csvAdapter = new CsvDataAdapter();
        JsonDataAdapter jsonAdapter = new JsonDataAdapter();

        DataHandler handler = new DataHandler(List.of(csvAdapter, jsonAdapter));

        DataLogger log = new DataLogger(handler);

        Thread.sleep(1000);
    }
}
