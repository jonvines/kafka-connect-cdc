package io.confluent.kafka.connect.cdc;

public interface ChangeWriter {
  void addChange(Change change);
}
