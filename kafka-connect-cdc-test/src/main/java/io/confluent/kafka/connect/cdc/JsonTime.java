package io.confluent.kafka.connect.cdc;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import org.apache.kafka.common.utils.Time;

import java.io.IOException;
import java.util.Date;

import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.*;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY, getterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class JsonTime {
  long milliseconds;

  static class Serializer extends JsonSerializer<Time> {
    @Override
    public void serialize(Time time, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException, JsonProcessingException {
      JsonTime result = new JsonTime();
      result.milliseconds = time.milliseconds();
      jsonGenerator.writeObject(result);
    }
  }

  static class Deserializer extends JsonDeserializer<Time> {
    @Override
    public Time deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException, JsonProcessingException {
      JsonTime storage = jsonParser.readValueAs(JsonTime.class);
      Time result = mock(Time.class);
      when(result.milliseconds()).thenReturn(storage.milliseconds);
      when(result.nanoseconds()).thenReturn(storage.milliseconds * 1000);
      doAnswer(invocationOnMock -> {
        Long l = invocationOnMock.getArgumentAt(0, Long.class);
        Thread.sleep(l);
        return null;
      }).when(result).sleep(anyLong());
      return result;
    }
  }
}
