package org.apache.phoenix.replication.schema;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.ReflectionUtils;


public final class SchemaRepositoryFactory {
  private static final String SCHEMA_REPO_CONF_KEY = "phoenix.schema.repository";
  public static SchemaRepository create(Configuration conf) {
    Class<? extends SchemaRepository> clazz = conf.getClass(
        SCHEMA_REPO_CONF_KEY, StaticSchemaRepository.class, SchemaRepository.class);
    return ReflectionUtils.newInstance(clazz);
  }
}
