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

import org.apache.kafka.connect.data.Struct;

import java.util.Map;

class StructPair implements Map.Entry<Struct, Struct> {
  private final Struct key;
  private final Struct value;

  public StructPair(SchemaPair schemaPair) {
    this.key = new Struct(schemaPair.getKey().schema);
    this.value = new Struct(schemaPair.getValue().schema);
  }

  @Override
  public Struct getKey() {
    return this.key;
  }

  @Override
  public Struct getValue() {
    return this.value;
  }

  @Override
  public Struct setValue(Struct value) {
    throw new UnsupportedOperationException("setValue is not supported.");
  }
}
