/**
 * Copyright © 2017 Jeremy Custenborder (jcustenborder@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.jcustenborder.kafka.connect.cdc;

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
