package io.latent.storm.rabbitmq;

public interface ErrorReporter {
  void reportError(Throwable error);
}
