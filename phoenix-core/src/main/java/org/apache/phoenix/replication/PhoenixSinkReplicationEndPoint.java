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

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.RegionServerCoprocessorEnvironment;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos;
import org.apache.phoenix.coprocessor.generated.PhoenixReplicationSinkUtil.PhoenixReplicationSinkService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class PhoenixSinkReplicationEndPoint extends PhoenixReplicationSinkService
    implements CoprocessorService, Coprocessor {

  private static final Logger LOG =
      LoggerFactory.getLogger(PhoenixSinkReplicationEndPoint.class);
  private RegionServerCoprocessorEnvironment env;
  @Override
  public void start(CoprocessorEnvironment env) throws IOException {
    if (env instanceof RegionServerCoprocessorEnvironment) {
      this.env = (RegionServerCoprocessorEnvironment) env;
    } else {
      throw new CoprocessorException("Must be loaded on a region server!");
    }
  }

  @Override
  public void stop(CoprocessorEnvironment env) {

  }

  @Override
  public Service getService() {
    return this;
  }

  @Override
  public void replicateEntries(RpcController controller, AdminProtos.ReplicateWALEntryRequest request,
     RpcCallback<AdminProtos.ReplicateWALEntryResponse> done) {
    List<AdminProtos.WALEntry> entries = request.getEntryList();
    for (AdminProtos.WALEntry entry: entries) {
      LOG.info("Received WAL entry: " + entry.toString());
    }
  }
}
