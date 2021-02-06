/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.keygen;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;

/**
 * A KeyGenerator which use the uuid as the record key.
 */
public class UuidKeyGenerator extends BuiltinKeyGenerator {

  public UuidKeyGenerator(TypedProperties props) {
    super(props);
    this.recordKeyFields = new ArrayList<>();
    this.partitionPathFields = Arrays.stream(props.getString(KeyGeneratorOptions.PARTITIONPATH_FIELD_OPT_KEY)
      .split(",")).filter(s -> s.length() > 0).map(String::trim).collect(Collectors.toList());
  }

  @Override
  public String getRecordKey(GenericRecord record) {
    return UUID.randomUUID().toString();
  }

  @Override
  public String getPartitionPath(GenericRecord record) {
    return KeyGenUtils.getRecordPartitionPath(record, getPartitionPathFields(),
      hiveStylePartitioning, encodePartitionPath);
  }
}

