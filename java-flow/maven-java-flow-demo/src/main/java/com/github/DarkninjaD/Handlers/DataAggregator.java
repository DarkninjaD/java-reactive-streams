package com.github.DarkninjaD.Handlers;

import java.util.concurrent.Flow;
import java.util.concurrent.SubmissionPublisher;

import com.github.DarkninjaD.Interfaces.IDataProvider;

public class DataAggregator implements Flow.Publisher<String> {

  // The publisher that talks to our final downstream subscriber
  private final SubmissionPublisher<String> downstreamPublisher = new SubmissionPublisher<>();

  // A method to attach as many upstream sources as we want
  public void addSource(IDataProvider provider) {
    provider.getPublisher().subscribe(new Flow.Subscriber<String>() {
      private Flow.Subscription subscription;

      @Override
      public void onSubscribe(Flow.Subscription subscription) {
        this.subscription = subscription;
        // Request the first piece of data (Backpressure in action!)
        subscription.request(1);
      }

      @Override
      public void onNext(String item) {
        // Here is our "Processing" step
        String processedData = "[PROCESSED BY AGGREGATOR] " + item.toUpperCase();

        // Push to downstream
        downstreamPublisher.submit(processedData);

        // Request the next piece of data
        subscription.request(1);
      }

      @Override
      public void onError(Throwable throwable) {
        System.err.println("Source Error: " + throwable.getMessage());
      }

      @Override
      public void onComplete() {
        // Notice we don't close the downstream publisher here,
        // because other sources might still be sending data!
        System.out.println("A source has finished emitting data.");
      }
    });
  }

  // Fulfill the Flow.Publisher interface so our final Subscriber can attach to
  // this
  @Override
  public void subscribe(Flow.Subscriber<? super String> subscriber) {
    downstreamPublisher.subscribe(subscriber);
  }

  public void close() {
    downstreamPublisher.close();
  }
}
