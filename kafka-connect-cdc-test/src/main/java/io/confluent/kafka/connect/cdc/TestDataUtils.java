package io.confluent.kafka.connect.cdc;

import com.google.common.base.Preconditions;
import com.google.common.io.Files;
import org.reflections.Reflections;
import org.reflections.scanners.ResourcesScanner;
import org.reflections.util.FilterBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;

public class TestDataUtils {
  private static final Logger log = LoggerFactory.getLogger(TestDataUtils.class);

  static Random random = new SecureRandom();

  public static BigDecimal randomBigDecimal(int scale) {
    long longValue = random.nextLong();
    return BigDecimal.valueOf(longValue, scale);
  }

  public static <T extends NamedTest> List<T> loadJsonResourceFiles(String packageName, Class<T> cls) throws IOException {
    Preconditions.checkNotNull(packageName, "packageName cannot be null");
//    Preconditions.checkState(packageName.startsWith("/"), "packageName must start with a /.");

    Reflections reflections = new Reflections(packageName, new ResourcesScanner());

    Set<String> resources = reflections.getResources(new FilterBuilder.Include(".*"));
    List<T> datas = new ArrayList<>(resources.size());

    Path packagePath = Paths.get("/" + packageName.replace(".", "/"));

    for (String resource : resources) {
      log.trace("Loading resource {}", resource);
      Path resourcePath = Paths.get("/" + resource);
      Path relativePath = packagePath.relativize(resourcePath);
      File resourceFile = new File("/" + resource);
      T data;
      try (InputStream inputStream = cls.getResourceAsStream(resourceFile.getAbsolutePath())) {
        data = ObjectMapperFactory.instance.readValue(inputStream, cls);
      } catch (IOException ex) {
        if (log.isErrorEnabled()) {
          log.error("Exception thrown while loading {}", resourcePath, ex);
        }
        throw ex;
      }

      String nameWithoutExtension = Files.getNameWithoutExtension(resource);
      if (null != relativePath.getParent()) {
        String parentName = relativePath.getParent().getFileName().toString();
        data.name(parentName + "/" + nameWithoutExtension);
      } else {
        data.name(nameWithoutExtension);
      }

      datas.add(data);
    }

    return datas;
  }
}
