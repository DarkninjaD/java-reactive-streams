package com.github.DarkninjaD.Adapters;

import java.util.concurrent.Flow;
import java.util.concurrent.SubmissionPublisher;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.github.DarkninjaD.Interfaces.IDataProvider;

public class AsyncSource implements IDataProvider {
    private final String sourceName;
    // SubmissionPublisher is standard Java's out-of-the-box Publisher
    // implementation
    private final SubmissionPublisher<String> publisher = new SubmissionPublisher<>();
    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

    public AsyncSource(String sourceName, int delayMs) {
        this.sourceName = sourceName;
        startProducingData(delayMs);
    }

    private void startProducingData(int delayMs) {
        // Run a task asynchronously every 'delayMs' milliseconds
        Runnable task = new Runnable() {
            int counter = 1;

            @Override
            public void run() {
                String data = sourceName + " - Item " + counter++;
                // .submit() pushes data downstream asynchronously
                publisher.submit(data);

                // Stop after 5 items for this demo
                if (counter > 5) {
                    publisher.close();
                    executor.shutdown();
                }
            }
        };
        executor.scheduleAtFixedRate(task, 0, delayMs, TimeUnit.MILLISECONDS);
    }

    @Override
    public Flow.Publisher<String> getPublisher() {
        return publisher;
    }
}