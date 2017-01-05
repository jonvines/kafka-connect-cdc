package io.confluent.kafka.connect.cdc;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.storage.OffsetStorageReader;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public abstract class CachingTableMetadataProvider<T extends PooledCDCSourceConnectorConfig> implements TableMetadataProvider {
  protected final T config;
  protected final OffsetStorageReader offsetStorageReader;
  final Cache<ChangeKey, TableMetadata> tableMetadataCache;
  protected Map<ChangeKey, Map<String, Object>> cachedOffsets = new HashMap<>();

  public CachingTableMetadataProvider(T config, OffsetStorageReader offsetStorageReader) {
    this.config = config;
    this.tableMetadataCache = CacheBuilder
        .newBuilder()
        .expireAfterWrite(config.schemaCacheMs, TimeUnit.MILLISECONDS)
        .build();
    this.offsetStorageReader = offsetStorageReader;
  }

  @Override
  public void cacheOffset(ChangeKey changeKey, Map<String, Object> offset) {
    cachedOffsets.put(changeKey, offset);
  }


  protected abstract TableMetadata fetchTableMetadata(ChangeKey changeKey) throws SQLException;

  @Override
  public TableMetadata tableMetadata(ChangeKey changeKey) {
    try {
      return this.tableMetadataCache.get(changeKey, new Callable<TableMetadata>() {
        @Override
        public TableMetadata call() throws Exception {
          return fetchTableMetadata(changeKey);
        }
      });

    } catch (ExecutionException ex) {
      throw new DataException("Exception thrown while getting metadata for " + changeKey, ex);
    }
  }
}
