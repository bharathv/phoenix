package org.apache.phoenix.replication.schema;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;

import java.util.HashMap;
import java.util.Map;

public class StaticSchemaRepository implements SchemaRepository{
  public static final Map<String, String>  tableMapping = new HashMap<>();
  @Override
  public void init(Configuration conf) throws Exception {
    tableMapping.put("t1", "pt1");
    tableMapping.put("tableName", "ptableName");
    tableMapping.put("t2", "pt2");
    tableMapping.put("test", "TEST");
  }

  @Override
  public byte[] resolve(TableName tableName) {
    String table = tableName.toString();
    if (!tableMapping.containsKey(table)) {
      return null;
    }
    return tableMapping.get(table).getBytes();
  }

  @Override
  public void stop() {

  }
}
