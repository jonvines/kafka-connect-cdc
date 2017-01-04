package io.confluent.kafka.connect.cdc;

import com.google.common.base.MoreObjects;
import org.apache.kafka.connect.data.Schema;

public class Utils {

  public static String toString(Schema schema) {
    return MoreObjects.toStringHelper(Schema.class)
        .omitNullValues()
        .add("type", schema.type())
        .add("name", schema.name())
        .toString();
  }

}
