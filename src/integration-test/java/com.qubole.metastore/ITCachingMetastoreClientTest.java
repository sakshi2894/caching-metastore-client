package com.qubole.metastore;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.primitives.Bytes;
import com.qubole.utility.JdkSerializer;

import org.apache.commons.lang.SerializationUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by sakshibansal on 25/07/17.
 */
public class ITCachingMetastoreClientTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(ITCachingMetastoreClientTest.class);
  private static final String BAD_DATABASE = "baddb";
  private static final String BAD_TABLE = "badtb";
  private static CachingMetastoreClient cachingMetastoreClient;
  private static HiveMetaStoreClient hiveMetastoreClient;
  private static Jedis redis;
  private static String uuid = UUID.randomUUID().toString().replace("-", "");
  private static String PREFIX = "cachingtest_" + uuid;

  @BeforeClass
  public static void setUp() throws Exception {
    String username = System.getProperty("USERNAME");
    String password = System.getProperty("PASSWORD");
    String dbName = System.getProperty("DB_NAME");
    String dbURL = System.getProperty("METASTORE_URL");
    String redisEndpoint = System.getProperty("REDIS_ENDPOINT");


    HiveConf hiveConf = new HiveConf();
    hiveConf.set("javax.jdo.option.ConnectionURL", String.format("jdbc:mysql://%s:3306/%s", dbURL, dbName));
    hiveConf.set("javax.jdo.option.ConnectionUserName", username);
    hiveConf.set("jdo.option.ConnectionPassword", password);
    hiveConf.set("javax.jdo.option.ConnectionDriverName", "com.mysql.jdbc.Driver");
    hiveMetastoreClient = new HiveMetaStoreClient(hiveConf);
    int cacheTtlMinutes = 20;
    cachingMetastoreClient = new CachingMetastoreClient(
            redisEndpoint, PREFIX, cacheTtlMinutes, hiveMetastoreClient, cacheTtlMinutes, true);
    setupSchema();

    JedisPool redisPool = new JedisPool(new JedisPoolConfig(),
            redisEndpoint, 6379, Protocol.DEFAULT_TIMEOUT);
    redis = redisPool.getResource();
  }

  //  public StorageDescriptor(List<FieldSchema> cols, String location, String inputFormat, String outputFormat, boolean compressed, int numBuckets, SerDeInfo serdeInfo, List<String> bucketCols, List<Order> sortCols, Map<String, String> parameters) {
  public static void createTable (String tableName, String dbName, List<FieldSchema> columnList, Map<String, String> parameters, List<FieldSchema> partitionList, String inputFormat, String outputFormat) throws Exception {
    StorageDescriptor descriptor = new StorageDescriptor(columnList, "", inputFormat, outputFormat, false, -1, null, null, null, null );
    hiveMetastoreClient.createTable(new Table(tableName, dbName, "", -1, -1, -1, descriptor, partitionList, parameters, "", "", ""));
  }

  public static void setupSchema() throws Exception {

    hiveMetastoreClient.dropDatabase("test_db", true, true, true);
    hiveMetastoreClient.dropDatabase("test_db2", true, true, true);
    hiveMetastoreClient.createDatabase(new Database("test_db", "", "", null));
    hiveMetastoreClient.createDatabase(new Database("test_db2", "", "", null));

    createTable("students", "test_db", null, null, null, "", "");
    createTable("studentsf", "test_db", null, null, null, "", "");

    //create test_db.class
    List<FieldSchema> classColumns = new ArrayList<>();
    classColumns.add(new FieldSchema("id", "float", ""));
    classColumns.add(new FieldSchema("name", "string",""));
    classColumns.add(new FieldSchema("class", "string",""));

    createTable("class", "test_db", classColumns, null, null, "", "");


    createTable("students", "test_db2", null, null, null, "", "");
    createTable("studentsf", "test_db2", null, null, null, "", "");

    List<FieldSchema> marksColumns = new ArrayList<>();
    marksColumns.add(new FieldSchema("id", "int", ""));
    marksColumns.add(new FieldSchema("name", "string",""));
    marksColumns.add(new FieldSchema("marks", "int",""));

    List<FieldSchema> marksPartitions = new ArrayList<>();
    marksPartitions.add(new FieldSchema("subject", "string", ""));
    marksPartitions.add(new FieldSchema("date", "string", ""));

    Map<String, String> parameters = new HashMap<>();
    parameters.put("transient_lastDdlTime", "12321");

    createTable("marks", "test_db2", marksColumns, parameters, marksPartitions, "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat", "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat");
  }

  @AfterClass
  public static void clearSchema() throws Exception {

    /**
    String api_endpoint = "https://" + endpoint + "/api";
    QdsConfiguration configuration = new DefaultQdsConfiguration(api_endpoint, account_auth);
    QdsClient client = QdsClientFactory.newClient(configuration);
    String query = "drop database if exists test_db CASCADE;" +
            "drop database if exists test_db2 CASCADE;";

    try {
      CommandResponse commandResponse = client.command().hive().query(query).invoke().get();
      ResultLatch resultLatch = new ResultLatch(client, commandResponse.getId());
      resultLatch.awaitResult();
    } finally {
      client.close();
    }**/
  }


  @Test
  public void testGetTables() throws Exception {
    String dbName = "test_db2";
    String tblPattern = "student.*";

    String cacheKey = PREFIX + ".tableNamesCache." + dbName;

    assertThat(redis.exists(cacheKey)).isFalse();
    int preMissCount = cachingMetastoreClient.getCachedStats().getKeyspaceMisses();
    cachingMetastoreClient.getTables(dbName, tblPattern);
    int postMissCount = cachingMetastoreClient.getCachedStats().getKeyspaceMisses();
    assertThat(postMissCount - preMissCount).isEqualTo(1);

    assertThat(redis.exists(cacheKey)).isTrue();

    int preHitCount = cachingMetastoreClient.getCachedStats().getKeyspaceHits();
    List<String> tb_observed = cachingMetastoreClient.getTables(dbName, tblPattern);
    int postHitCount = cachingMetastoreClient.getCachedStats().getKeyspaceHits();
    assertThat(postHitCount - preHitCount).isEqualTo(1);


    ImmutableList.Builder<String> tb_expectedBuilder = new ImmutableList.Builder<>();
    tb_expectedBuilder.add("students");
    tb_expectedBuilder.add("studentsf");

    ImmutableList<String> tb_expected = tb_expectedBuilder.build();

    assertThat(tb_observed.size()).isEqualTo(tb_expected.size());
    for (String tableName : tb_expected) {
      assertThat(tb_observed).contains(tableName);
    }
  }

  @Test(expected=CachingMetastoreException.class)
  public void testInvalidGetTables() throws Exception {
    cachingMetastoreClient.getTables(BAD_DATABASE, "*.*");
  }

  @Test
  public void testGetAllTables() throws Exception {
    String dbName = "test_db";
    String cacheKey = PREFIX + ".tableNamesCache." + dbName;

    assertThat(redis.exists(cacheKey)).isFalse();
    int preMissCount = cachingMetastoreClient.getCachedStats().getKeyspaceMisses();
    cachingMetastoreClient.getAllTables(dbName);
    int postMissCount = cachingMetastoreClient.getCachedStats().getKeyspaceMisses();
    assertThat(postMissCount - preMissCount).isEqualTo(1);

    assertThat(redis.exists(cacheKey)).isTrue();

    int preHitCount = cachingMetastoreClient.getCachedStats().getKeyspaceHits();
    List<String> tb_observed = cachingMetastoreClient.getAllTables(dbName);
    int postHitCount = cachingMetastoreClient.getCachedStats().getKeyspaceHits();
    assertThat(postHitCount - preHitCount).isEqualTo(1);


    ImmutableList.Builder<String> tb_expectedBuilder = new ImmutableList.Builder<>();
    tb_expectedBuilder.add("students");
    tb_expectedBuilder.add("studentsf");
    tb_expectedBuilder.add("class");

    ImmutableList<String> tb_expected = tb_expectedBuilder.build();

    assertThat(tb_observed.size()).isEqualTo(tb_expected.size());
    for (String tableName : tb_expected) {
      assertThat(tb_observed).contains(tableName);
    }
  }

  @Test(expected=CachingMetastoreException.class)
  public void testInvalidGetAllTables() throws Exception {
    cachingMetastoreClient.getAllTables(BAD_DATABASE);
  }

  @Test
  public void testGetTable() throws Exception {
    String dbName = "test_db2";
    String tbName = "marks";

    CachingMetastoreClient.HiveTableName hiveTableName =
            new CachingMetastoreClient.HiveTableName(dbName, tbName);
    byte[] cacheKey = Bytes.concat((PREFIX + ".tableCache.").getBytes(), SerializationUtils.serialize(hiveTableName));


    assertThat(redis.exists(cacheKey)).isFalse();
    int preMissCount = cachingMetastoreClient.getCachedStats().getKeyspaceMisses();
    cachingMetastoreClient.getTable(dbName, tbName);
    int postMissCount = cachingMetastoreClient.getCachedStats().getKeyspaceMisses();
    assertThat(postMissCount - preMissCount).isEqualTo(1);

    assertThat(redis.exists(cacheKey)).isTrue();

    int preHitCount = cachingMetastoreClient.getCachedStats().getKeyspaceHits();
    Table table = cachingMetastoreClient.getTable(dbName, tbName);
    int postHitCount = cachingMetastoreClient.getCachedStats().getKeyspaceHits();
    assertThat(postHitCount - preHitCount).isEqualTo(1);


    assertThat(table.getDbName()).isEqualTo("test_db2");
    assertThat(table.getTableName()).isEqualTo("marks");

    assertThat(table.getSd().getInputFormat()).isEqualTo("org.apache.hadoop.hive.ql.io.orc.OrcInputFormat");
    assertThat(table.getSd().getOutputFormat()).isEqualTo("org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat");

    assertThat(table.getParameters().get("transient_lastDdlTime")).isNotNull();

    List<FieldSchema> columns_expected = Lists.newArrayList();
    columns_expected.add(new FieldSchema("id", "int", null));
    columns_expected.add(new FieldSchema("name", "string", null));
    columns_expected.add(new FieldSchema("marks", "int", null));

    List<FieldSchema> columns_observed = table.getSd().getCols();
    compareFieldList(columns_observed, columns_expected);

    List<FieldSchema> partitions_expected = Lists.newArrayList();
    partitions_expected.add(new FieldSchema("subject", "string", null));
    partitions_expected.add(new FieldSchema("date", "string", null));

    List<FieldSchema> partitions_observed = table.getPartitionKeys();
    compareFieldList(partitions_observed, partitions_expected);
  }

  @Test(expected=CachingMetastoreException.class)
  public void testInvalidGetTable() throws Exception {
    String dbName = "test_db";
    cachingMetastoreClient.getTable(dbName, "badtb2");
  }

  @Test
  public void testGetFields() throws Exception {
    String dbName = "test_db";
    String tbName = "class";

    CachingMetastoreClient.HiveTableName hiveTableName =
            new CachingMetastoreClient.HiveTableName(dbName, tbName);
    byte[] cacheKey = Bytes.concat((PREFIX + ".tableCache.").getBytes(), SerializationUtils.serialize(hiveTableName));

    assertThat(redis.exists(cacheKey)).isFalse();
    int preMissCount = cachingMetastoreClient.getCachedStats().getKeyspaceMisses();
    cachingMetastoreClient.getFields(dbName, tbName);
    int postMissCount = cachingMetastoreClient.getCachedStats().getKeyspaceMisses();
    assertThat(postMissCount - preMissCount).isEqualTo(1);

    assertThat(redis.exists(cacheKey)).isTrue();

    int preHitCount = cachingMetastoreClient.getCachedStats().getKeyspaceHits();
    List<FieldSchema> columns_observed = cachingMetastoreClient.getFields(dbName, tbName);
    int postHitCount = cachingMetastoreClient.getCachedStats().getKeyspaceHits();
    assertThat(postHitCount - preHitCount).isEqualTo(1);


    List<FieldSchema> columns_expected = new ArrayList<>();
    columns_expected.add(new FieldSchema("id", "float", null));
    columns_expected.add(new FieldSchema("name", "string", null));
    columns_expected.add(new FieldSchema("class", "string", null));

    compareFieldList(columns_observed, columns_expected);
  }

  @Test(expected=CachingMetastoreException.class)
  public void testInvalidGetFields() throws Exception {
    String dbName = "test_db";
    cachingMetastoreClient.getFields(dbName, BAD_TABLE);
  }

  @Test
  public void testTableSerialization() throws Exception {

    Table table = hiveMetastoreClient.getTable("test_db2", "marks");

    JdkSerializer<Table> tableSerializer = new JdkSerializer<>();
    byte[] serialized = tableSerializer.serialize(table);
    Object deserialized = tableSerializer.deserialize(serialized);
    Table processedTable = (Table) deserialized;

    assertThat(processedTable.getDbName()).isEqualTo(table.getDbName());
    assertThat(processedTable.getTableName()).isEqualTo(table.getTableName());

    assertThat(processedTable.getSd().getInputFormat()).isEqualTo(table.getSd().getInputFormat());
    assertThat(processedTable.getSd().getOutputFormat()).isEqualTo(table.getSd().getOutputFormat());

    assertThat(table.getParameters().get("transient_lastDdlTime")).isNotNull();
    assertThat(processedTable.getParameters().get("transient_lastDdlTime")).isNotNull();


    List<FieldSchema> columns_observed = processedTable.getSd().getCols();
    List<FieldSchema> columns_expected = table.getSd().getCols();
    compareFieldList(columns_observed, columns_expected);


    List<FieldSchema> partitions_observed = processedTable.getPartitionKeys();
    List<FieldSchema> partitions_expected = table.getPartitionKeys();
    compareFieldList(partitions_observed, partitions_expected);

  }

  @Test
  public void testTableNotFound() throws Exception {
    String dbName = "baddb2";
    String tbName = BAD_TABLE;

    CachingMetastoreClient.HiveTableName hiveTableName =
            new CachingMetastoreClient.HiveTableName(dbName, tbName);
    byte[] cacheKey = Bytes.concat(("Doesn'tExist." + PREFIX + ".tableCache.").getBytes(), SerializationUtils.serialize(hiveTableName));

    assertThat(redis.exists(cacheKey)).isFalse();
    int preMissCount = cachingMetastoreClient.getCachedStats().getKeyspaceMisses();
    try {
      cachingMetastoreClient.getTable(dbName, tbName);
    } catch (Exception e) {
    }
    int postMissCount = cachingMetastoreClient.getCachedStats().getKeyspaceMisses();
    assertThat(postMissCount - preMissCount).isEqualTo(1);

    assertThat(redis.exists(cacheKey)).isTrue();

    int preHitCount = cachingMetastoreClient.getCachedStats().getKeyspaceHits();
    try {
      cachingMetastoreClient.getTable(dbName, tbName);
    } catch (Exception e) {
    }
    int postHitCount = cachingMetastoreClient.getCachedStats().getKeyspaceHits();
    assertThat(postHitCount - preHitCount).isEqualTo(0);

  }


  private void compareFieldList(List<FieldSchema> columns_observed, List<FieldSchema> columns_expected) {
    Collections.sort(columns_observed, new ITCachingMetastoreClientTest.columnComparator());
    Collections.sort(columns_expected, new ITCachingMetastoreClientTest.columnComparator());

    assertThat(columns_observed.size()).isEqualTo(columns_expected.size());
    for (int i = 0; i < columns_expected.size(); i++) {
      assertThat(columns_observed.get(i).getName()).isEqualTo(columns_expected.get(i).getName());
      assertThat(columns_observed.get(i).getType()).isEqualTo(columns_expected.get(i).getType());
    }
  }

  private class columnComparator implements Comparator<FieldSchema> {
    @Override
    public int compare(FieldSchema col1, FieldSchema col2) {
      return col1.getName().compareTo(col2.getName());
    }
  }

}


