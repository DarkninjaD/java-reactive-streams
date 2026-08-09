package com.github.DarkninjaD.Subscribers;

import java.util.concurrent.Flow;

public class LoggerSubscriber implements Flow.Subscriber<String> {
    private Flow.Subscription subscription;

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
        System.out.println("LoggerSub: Subscribed and requesting data...");
        this.subscription = subscription;
        // Request the first item
        subscription.request(1);
    }

    @Override
    public void onNext(String item) {
        System.out.println("LoggerSub: " + item);

        // Emulate some slight delay in our logger, then request next item
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
        }
        subscription.request(1);
    }

    @Override
    public void onError(Throwable throwable) {
        System.err.println("LoggerSub Error: " + throwable.getMessage());
    }

    @Override
    public void onComplete() {
        System.out.println("LoggerSub: All data processing complete!");
    }
}
