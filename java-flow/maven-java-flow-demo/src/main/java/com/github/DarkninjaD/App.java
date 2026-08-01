package com.github.DarkninjaD;

import com.github.DarkninjaD.Adapters.AsyncSource;
import com.github.DarkninjaD.Handlers.DataAggregator;
import com.github.DarkninjaD.Subscribers.LoggerSubscriber;

public class App {
    public static void main(String[] args) throws InterruptedException {

        AsyncSource sensorAlpha = new AsyncSource("Sensor Alpha", 300);
        AsyncSource sensorBravo = new AsyncSource("Sensor Bravo", 500);
        AsyncSource sensorCharlie = new AsyncSource("Sensor Charlie", 1000);

        DataAggregator aggregator = new DataAggregator();

        LoggerSubscriber logger = new LoggerSubscriber();

        aggregator.subscribe(logger);

        aggregator.addSource(sensorAlpha);
        aggregator.addSource(sensorBravo);
        aggregator.addSource(sensorCharlie);

        Thread.sleep(4000);

        aggregator.close();
    }
}
