package io.confluent.kafka.connect.cdc;

public interface NamedTest {
  void name(String name);

  String name();
}
