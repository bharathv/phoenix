/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.replication;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.phoenix.replication.schema.SchemaRepository;
import org.apache.phoenix.replication.schema.SchemaRepositoryFactory;

import java.io.IOException;
import java.util.List;

public class PhoenixSourceReplicationEndPoint extends HBaseInterClusterReplicationEndpoint {
  private SchemaRepository schemaRepository;
  private static final String TABLE_NAME_ATTRIBUTE = "phoenix.tablename";

  @Override
  public void init(Context context) throws IOException {
    super.init(context);
    schemaRepository = SchemaRepositoryFactory.create(context.getConfiguration());
  }

  @Override
  protected List<List<WAL.Entry>> createBatches(final List<WAL.Entry> entries) {
    // intercept the entries and add annotations.
    for (WAL.Entry entry: entries) {
      TableName hbaseTbl = entry.getKey().getTablename();
      entry.getKey().addExtendedAttribute(
          TABLE_NAME_ATTRIBUTE, schemaRepository.resolve(hbaseTbl));
    }
    return super.createBatches(entries);
  }

  @Override
  protected void doStop() {
    schemaRepository.stop();
    super.doStop();
  }
}
