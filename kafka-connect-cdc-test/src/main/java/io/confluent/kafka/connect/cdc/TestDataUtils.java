package io.confluent.kafka.connect.cdc;

import java.math.BigDecimal;
import java.security.SecureRandom;
import java.util.Random;

public class TestDataUtils {
  static Random random = new SecureRandom();

  public static BigDecimal randomBigDecimal(int scale) {
    long longValue = random.nextLong();
    return BigDecimal.valueOf(longValue, scale);
  }

}
