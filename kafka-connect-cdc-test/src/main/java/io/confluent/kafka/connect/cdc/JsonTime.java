package io.confluent.kafka.connect.cdc;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.kafka.common.utils.Time;

import java.util.Date;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY, getterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class JsonTime implements Time {
  public JsonTime() {

  }

  public JsonTime(long milliseconds) {
    this.milliseconds = milliseconds;
  }

  public JsonTime(Date date) {
    this.milliseconds = date.getTime();
  }

  long milliseconds;

  @Override
  public long milliseconds() {
    return this.milliseconds;
  }

  @Override
  public long nanoseconds() {
    return this.milliseconds * 1000;
  }

  @Override
  public void sleep(long l) {
    try {
      Thread.sleep(l);
    } catch (InterruptedException e) {

    }
  }
}
