package org.apache.phoenix.replication.schema;

import org.apache.hadoop.conf.Configuration;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.parse.TableName;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.schema.TableNotFoundException;
import org.apache.phoenix.util.PhoenixRuntime;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class PhoenixSchemaRepository implements SchemaRepository{
  private PhoenixConnection phoenixConn;
  @Override
  public void init(Configuration conf) throws SQLException  {
    phoenixConn = (PhoenixConnection) DriverManager.getConnection(conf.get("phoenix.jdbc.url",
        PhoenixRuntime.JDBC_PROTOCOL+ PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR + "localhost"));
  }

  @Override
  public byte[] resolve(org.apache.hadoop.hbase.TableName tableName) {
    PTableKey tableKey = new PTableKey(null, tableName.getNameAsString());
    try {
      PTable phoenixTable = phoenixConn.getTable(tableKey);
      return phoenixTable.getTableName().getBytes();
    } catch (TableNotFoundException tnfe) {
    }
    return null;
  }

  @Override
  public void stop() {
    if (phoenixConn == null) return;
    try {
      phoenixConn.close();
    } catch (Exception e) {

    }
  }
}
