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

import com.google.common.base.Preconditions;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.RegionServerCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.SingletonCoprocessorService;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.phoenix.coprocessor.generated.PhoenixReplicationSinkUtil.PhoenixReplicationSinkService;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.index.IndexMetaDataCacheClient;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.replication.schema.StaticSchemaRepository;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.schema.TableNotFoundException;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.QueryUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

public class PhoenixSinkReplicationEndPoint extends PhoenixReplicationSinkService
    implements SingletonCoprocessorService, Coprocessor {

  private static final Logger LOG =
      LoggerFactory.getLogger(PhoenixSinkReplicationEndPoint.class);
  private RegionServerCoprocessorEnvironment env;
  PhoenixConnection connection;
  @Override
  public void start(CoprocessorEnvironment env) throws IOException {
    if (env instanceof RegionServerCoprocessorEnvironment) {
      this.env = (RegionServerCoprocessorEnvironment) env;
    } else {
      throw new CoprocessorException("Must be loaded on a region server!");
    }
  }

  private PhoenixConnection getConnection() {
    if (connection == null) {
      try {
        connection = (PhoenixConnection)QueryUtil.getConnection(env.getConfiguration());
      } catch (SQLException e) {
        LOG.error("Error making connection: ", e);
      }
    }
    return connection;
  }

  @Override
  public void stop(CoprocessorEnvironment env) {
    if (connection != null) {
      try {
        connection.close();
      } catch (Exception e) {
        LOG.error("Error closing phoenix connection. ", e);
      }
    }
  }

  @Override
  public Service getService() {
    return this;
  }

  private boolean isNewRowOrType(final Cell previousCell, final Cell cell) {
    return previousCell == null || previousCell.getTypeByte() != cell.getTypeByte() ||
        !CellUtil.matchingRow(previousCell, cell);
  }

  private java.util.UUID toUUID(final HBaseProtos.UUID uuid) {
    return new java.util.UUID(uuid.getMostSigBits(), uuid.getLeastSigBits());
  }

  private <K1, K2, V> List<V> addToHashMultiMap(Map<K1, Map<K2,List<V>>> map, K1 key1, K2 key2, V value) {
    Map<K2,List<V>> innerMap = map.get(key1);
    if (innerMap == null) {
      innerMap = new HashMap<K2, List<V>>();
      map.put(key1, innerMap);
    }
    List<V> values = innerMap.get(key2);
    if (values == null) {
      values = new ArrayList<V>();
      innerMap.put(key2, values);
    }
    values.add(value);
    return values;
  }

  private void convertToMutations(List<AdminProtos.WALEntry> entries, final CellScanner cells)
      throws IOException, SQLException, InterruptedException {
    if (entries.isEmpty()) return;
    // Map of table => list of Rows, grouped by cluster id, we only want to flushCommits once per
    // invocation of this method per table and cluster id.
    Map<TableName, Map<List<UUID>, List<Mutation>>> rowMap = new TreeMap<TableName, Map<List<UUID>,
        List<Mutation>>>();

    String phoenixTbl = null;
    for (AdminProtos.WALEntry entry : entries) {
      WALProtos.WALKey key = entry.getKey();
      TableName table = TableName.valueOf(key.getTableName().toByteArray());
      // Hack because source side attributes are not passed due to a bug.
      phoenixTbl = StaticSchemaRepository.tableMapping.get(table.toString());
      Cell previousCell = null;
      Mutation m = null;
      int count = entry.getAssociatedCellCount();
      for (int i = 0; i < count; i++) {
        // Throw index out of bounds if our cell count is off
        if (!cells.advance()) {
          throw new ArrayIndexOutOfBoundsException("Expected=" + count + ", index=" + i);
        }
        Cell cell = cells.current();
        Preconditions.checkState(!CellUtil.matchingQualifier(cell, WALEdit.BULK_LOAD));
        if (isNewRowOrType(previousCell, cell)) {
          // Create new mutation
          m =
              CellUtil.isDelete(cell) ? new Delete(cell.getRowArray(), cell.getRowOffset(),
                  cell.getRowLength()) : new Put(cell.getRowArray(), cell.getRowOffset(),
                  cell.getRowLength());
          List<UUID> clusterIds = new ArrayList<UUID>();
          for (HBaseProtos.UUID clusterId : entry.getKey().getClusterIdsList()) {
            clusterIds.add(toUUID(clusterId));
          }
          m.setClusterIds(clusterIds);
          addToHashMultiMap(rowMap, table, clusterIds, m);
        }
        if (CellUtil.isDelete(cell)) {
          ((Delete) m).addDeleteMarker(cell);
        } else {
          ((Put) m).add(cell);
        }
        previousCell = cell;
      }
    }
    Preconditions.checkNotNull(phoenixTbl);

    PhoenixConnection connection = getConnection();
    Preconditions.checkNotNull(connection);

    if (!rowMap.isEmpty()) {
      LOG.debug("Started replicating mutations.");

      ClusterConnection conn = env.getRegionServerServices().getConnection();
      // This is a very crude way of doing it, we need to implement a proper custom mutation plan.
      for (Map.Entry<TableName, Map<List<UUID>, List<Mutation>>> entry : rowMap.entrySet()) {
        List<Mutation> mutations = (List<Mutation>)entry.getValue().values().toArray()[0];
        PTable phoenixTable = PhoenixRuntime.getTable(connection, phoenixTbl);
        ImmutableBytesPtr indexMetadata = new ImmutableBytesPtr();
        phoenixTable.getIndexMaintainers(indexMetadata, connection);
        IndexMetaDataCacheClient.setMetaDataOnMutations(connection,
            connection.getTable(new PTableKey(null, phoenixTbl)), mutations, indexMetadata);
        HTable hbaseTbl = (HTable) conn.getTable(entry.getKey());
        hbaseTbl.batch(mutations);
      }
      LOG.debug("Finished replicating mutations.");
    }
  }

  @Override
  public void replicateEntries(RpcController controller, AdminProtos.ReplicateWALEntryRequest request,
     RpcCallback<AdminProtos.ReplicateWALEntryResponse> done) {
    try {
      convertToMutations(request.getEntryList(),
          ((HBaseRpcController)controller).cellScanner());
    } catch (Exception e) {
      controller.setFailed(e.getMessage());
      done.run(null);
    }
  }
}
