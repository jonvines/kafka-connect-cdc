package io.confluent.kafka.connect.cdc;

import com.google.common.collect.MapDifference;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.Map;
import java.util.function.Supplier;

public class MapDifferenceSupplier implements Supplier<String> {
  final MapDifference<?, ?> mapDifference;
  final String method;

  public MapDifferenceSupplier(MapDifference<?, ?> mapDifference, String method) {
    this.mapDifference = mapDifference;
    this.method = method;
  }

  @Override
  public String get() {
    try (Writer w = new StringWriter()) {
      try (BufferedWriter writer = new BufferedWriter(w)) {
        writer.append(String.format("Map for actual.%s() does not match expected.%s().", this.method, this.method));
        writer.newLine();
        Map<?, ? extends MapDifference.ValueDifference<?>> differences = mapDifference.entriesDiffering();
        if (!differences.isEmpty()) {
          writer.append("Keys with Differences");
          writer.newLine();
          for (Map.Entry<?, ? extends MapDifference.ValueDifference<?>> kvp : differences.entrySet()) {
            writer.append("  ");
            writer.append(kvp.getKey().toString());
            writer.newLine();

            writer.append("    expected:");
            writer.append(kvp.getValue().leftValue().toString());
            writer.newLine();

            writer.append("    actual:");
            writer.append(kvp.getValue().rightValue().toString());
            writer.newLine();
          }
        }

        Map<?, ?> entries = mapDifference.entriesOnlyOnLeft();
        writeEntries(writer, "Only in expected map", entries);

        Map<?, ?> onlyInActual = mapDifference.entriesOnlyOnRight();
        writeEntries(writer, "Only in actual map", onlyInActual);
      }
      return w.toString();
    } catch (IOException ex) {
      throw new IllegalStateException(ex);
    }
  }

  private void writeEntries(BufferedWriter writer, String header, Map<?, ?> entries) throws IOException {
    if (!entries.isEmpty()) {
      writer.append(header);
      writer.newLine();

      for (Map.Entry<?, ?> kvp : entries.entrySet()) {
        writer.append("  ");
        writer.append(kvp.getKey().toString());
        writer.append(": ");
        writer.append(kvp.getValue().toString());
        writer.newLine();
      }
      writer.newLine();
    }
  }
}
