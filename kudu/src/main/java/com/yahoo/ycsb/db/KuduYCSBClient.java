/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package com.yahoo.ycsb.db;

import com.stumbleupon.async.DeferredGroupException;
import com.stumbleupon.async.TimeoutException;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.StringByteIterator;
import org.kududb.ColumnSchema;
import org.kududb.Schema;
import org.kududb.client.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.Vector;

import static org.kududb.Type.STRING;

/**
 * Kudu client for YCSB framework
 * Example to load:
 * $ ./bin/ycsb load kudu -P workloads/kudu_workload -p sync_ops=false -threads 5
 * Example to run:
 * ./bin/ycsb run kudu -P workloads/kudu_workload -threads 5
 *
 */
public class KuduYCSBClient extends com.yahoo.ycsb.DB {
  public static final String KEY = "key";
  public static final int Ok = 0;
  public static final int ServerError = -1;
  public static final int HttpError = -2;
  public static final int NoMatchingRecord = -3;
  public static final int Timeout = -4;
  public static final int MAX_TABLETS = 9000;
  public static final long DEFAULT_SLEEP = 10000;
  private static final String DEBUG_OPT = "debug";
  private static final String PRINT_ROW_ERRORS_OPT = "print_row_errors";
  private static final String PRE_SPLIT_NUM_TABLETS_OPT = "pre_split_num_tablets";
  private static final String TABLE_NUM_REPLICAS = "table_num_replicas";
  private static final String TABLE_NAME_OPT = "table_name";
  private static final String DEFAULT_TABLE_NAME = "ycsb";
  private static final ColumnSchema keyColumn = new ColumnSchema.ColumnSchemaBuilder(KEY, STRING)
      .key(true)
      .build();
  private static KuduClient client;
  private static Schema schema;
  public boolean debug = false;
  public boolean printErrors = false;
  public String tableName;
  private KuduSession session;
  private KuduTable table;

  /**
   * Initialize any state for this DB.
   * Called once per DB instance; there is one DB instance per client thread.
   */
  public void init() throws DBException {
    if (getProperties().getProperty(DEBUG_OPT) != null) {
      this.debug = getProperties().getProperty(DEBUG_OPT).equals("true");
    }
    if (getProperties().getProperty(PRINT_ROW_ERRORS_OPT) != null) {
      this.printErrors = getProperties().getProperty(PRINT_ROW_ERRORS_OPT).equals("true");
    }
    if (getProperties().getProperty(PRINT_ROW_ERRORS_OPT) != null) {
      this.printErrors = getProperties().getProperty(PRINT_ROW_ERRORS_OPT).equals("true");
    }
    this.tableName = getProperties().getProperty(TABLE_NAME_OPT);
    if (this.tableName == null) {
       this.tableName = DEFAULT_TABLE_NAME;
    }
    initClient(debug, tableName, getProperties());
    this.session = client.newSession();
    this.session.setFlushMode(KuduSession.FlushMode.AUTO_FLUSH_BACKGROUND);
    this.session.setMutationBufferSpace(100);
    try {
      this.table = client.openTable(tableName);
    } catch (Exception e) {
      throw new DBException("Could not open a table because of:", e);
    }
    // do the lookups
    int returnCode;
    do {
      returnCode = this.read(tableName, "user", null, new HashMap<String, ByteIterator>());
      if (returnCode == Ok) {
        System.out.println("Still waiting on the table to be created");
      }
    } while (returnCode == Timeout);
  }

  private synchronized static void initClient(boolean debug, String tableName, Properties prop)
      throws DBException {
    if (client != null) return;

    String masterQuorum = prop.getProperty("masterQuorum");
    if (masterQuorum == null) {
      masterQuorum = "localhost:7051";
    }

    int numTablets = getIntFromProp(prop, PRE_SPLIT_NUM_TABLETS_OPT, 4);
    if (numTablets > MAX_TABLETS) {
      throw new DBException("Specified number of tablets (" + numTablets + ") must be equal " +
          "or below " + MAX_TABLETS);
    }

    int numReplicas = getIntFromProp(prop, TABLE_NUM_REPLICAS, 3);

    client = new KuduClient(masterQuorum);
    if (debug) {
      System.out.println("Connecting to the masters at " + masterQuorum);
    }
    client.setTimeoutMillis(DEFAULT_SLEEP);

    List<ColumnSchema> columns = new ArrayList<ColumnSchema>(11);
    columns.add(keyColumn);
    columns.add(new ColumnSchema.ColumnSchemaBuilder("field0", STRING).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("field1", STRING).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("field2", STRING).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("field3", STRING).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("field4", STRING).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("field5", STRING).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("field6", STRING).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("field7", STRING).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("field8", STRING).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("field9", STRING).build());
    schema = new Schema(columns);

    CreateTableBuilder builder = new CreateTableBuilder();
    builder.setNumReplicas(numReplicas);
    KeyBuilder keyBuilder = new KeyBuilder(schema);
    // create n-1 split keys, which will end up being n tablets master-side
    for (int i = 1; i < numTablets + 0; i++) {
      // We do +1000 since YCSB starts at user1.
      int startKeyInt = (MAX_TABLETS / numTablets * i) + 1000;
      String startKey = String.format("%04d", startKeyInt);
      builder.addSplitKey(keyBuilder.addString("user" + startKey));
    }

    try {
      client.createTable(tableName, schema, builder);
    } catch (Exception e) {
      if (!e.getMessage().contains("ALREADY_PRESENT")) {
        throw new DBException("Couldn't create the table", e);
      }
    }
  }

  private static int getIntFromProp(Properties prop, String propName, int defaultValue)
      throws DBException {
    String intStr = prop.getProperty(propName);
    if (intStr == null) {
      return defaultValue;
    } else {
      try {
        return Integer.valueOf(intStr);
      } catch (NumberFormatException ex) {
        throw new DBException("Provided number for " + propName + " isn't a valid integer");
      }
    }
  }

  /**
   * Cleanup any state for this DB.
   * Called once per DB instance; there is one DB instance per client thread.
   */
  public void cleanup() throws DBException {

    try {
      ArrayList<OperationResponse> responses = this.session.flush();
      this.session.close();
    } catch (Exception e) {
      System.err.println("Couldn't cleanup properly because: " + e);
    }
  }

  /**
   * Read a record from the database. Each field/value pair from the result will be stored in a HashMap.
   *
   * @param table  The name of the table
   * @param key    The record key of the record to read.
   * @param fields The list of fields to read, or null for all of them
   * @param result A HashMap of field/value pairs for the result
   * @return Zero on success, a non-zero error code on error
   */
  public int read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
    Vector<HashMap<String, ByteIterator>> results = new Vector<HashMap<String, ByteIterator>>();
    int ret = scan(table, key, 1, fields, results);
    if (ret != Ok) return ret;
    if (results.size() != 1) return NoMatchingRecord;
    result.putAll(results.firstElement());
    return Ok;
  }

  /**
   * Perform a range scan for a set of records in the database. Each field/value pair from the result will be stored in a HashMap.
   *
   * @param table       The name of the table
   * @param startkey    The record key of the first record to read.
   * @param recordcount The number of records to read
   * @param fields      The list of fields to read, or null for all of them
   * @param result      A Vector of HashMaps, where each HashMap is a set field/value pairs for one record
   * @return Zero on success, a non-zero error code on error
   */
  public int scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {

    try {

      Schema querySchema;
      if (fields == null) {
        querySchema = schema;
      } else {
        List<ColumnSchema> columns = new ArrayList<ColumnSchema>(fields.size() + 1);
        columns.add(new ColumnSchema.ColumnSchemaBuilder("key", STRING).build());
        for (String col : fields) { // TODO ugh do we need this for every single request?
          columns.add(new ColumnSchema.ColumnSchemaBuilder(col, STRING).build());
        }
        querySchema = new Schema(columns);
      }

      ColumnRangePredicate crp = new ColumnRangePredicate(keyColumn);
      crp.setLowerBound(startkey);
      if (recordcount == 1) {
        crp.setUpperBound(startkey);
      }

      KuduScanner scanner = client.newScannerBuilder(this.table, querySchema)
          .maxNumBytes(recordcount * querySchema.getRowSize())
          .limit(recordcount) // currently noop
          .addColumnRangePredicate(crp)
          .build();

      while (scanner.hasMoreRows()) {
        AsyncKuduScanner.RowResultIterator data = scanner.nextRows();
        addAllRowsToResult(data, recordcount, querySchema, result);
        if (recordcount == result.size()) break;
      }
      AsyncKuduScanner.RowResultIterator closer = scanner.close();
      addAllRowsToResult(closer, recordcount, querySchema, result);
    } catch (TimeoutException te) {
      if (printErrors) {
        System.err.println("Waited too long for a scan operation with start key=" + startkey);
      }
      return Timeout;
    } catch (Exception e) {
      System.err.println("Unexpected exception " + e);
      return ServerError;
    }
    return Ok;
  }

  private void addAllRowsToResult(AsyncKuduScanner.RowResultIterator it, int recordcount,
                                  Schema querySchema, Vector<HashMap<String, ByteIterator>> result)
      throws Exception {
    RowResult row;
    HashMap<String, ByteIterator> rowResult =
        new HashMap<String, ByteIterator>(querySchema.getColumnCount());
    if (it == null) return;
    while (it.hasNext()) {
      if (result.size() == recordcount) return;
      row = it.next();
      int colIdx = 0;
      for (ColumnSchema col : querySchema.getColumns()) {
        rowResult.put(col.getName(), new StringByteIterator(row.getString(colIdx)));
        colIdx++;
      }
      result.add(rowResult);
    }
  }

  /**
   * Update a record in the database. Any field/value pairs in the specified values HashMap will be written into the record with the specified
   * record key, overwriting any existing values with the same field name.
   *
   * @param table  The name of the table
   * @param key    The record key of the record to write
   * @param values A HashMap of field/value pairs to update in the record
   * @return Zero on success, a non-zero error code on error
   */
  public int update(String table, String key, HashMap<String, ByteIterator> values) {
    Update update = this.table.newUpdate();
    update.addString(KEY, key);
    for (int i = 1; i < schema.getColumnCount(); i++) {
      String columnName = schema.getColumn(i).getName();
      if (values.containsKey(columnName)) {
        String value = values.get(columnName).toString();
        update.addString(columnName, value);
      }
    }
    apply(update);
    return Ok;
  }

  /**
   * Insert a record in the database. Any field/value pairs in the specified values HashMap will be written into the record with the specified
   * record key.
   *
   * @param table  The name of the table
   * @param key    The record key of the record to insert.
   * @param values A HashMap of field/value pairs to insert in the record
   * @return Zero on success, a non-zero error code on error
   */
  public int insert(String table, String key, HashMap<String, ByteIterator> values) {
    Insert insert = this.table.newInsert();
    insert.addString(KEY, key);
    for (int i = 1; i < schema.getColumnCount(); i++) {
      insert.addString(schema.getColumn(i).getName(), new String(values.get(schema.getColumn(i)
          .getName()).toArray()));
    }
    apply(insert);
    return Ok;
  }

  /**
   * Delete a record from the database.
   *
   * @param table The name of the table
   * @param key   The record key of the record to delete.
   * @return Zero on success, a non-zero error code on error
   */
  public int delete(String table, String key) {
    Delete delete = this.table.newDelete();
    delete.addString(KEY, key);
    apply(delete);
    return Ok;
  }

  private void apply(Operation op) {
    try {
      session.apply(op);
    } catch (Exception ex) {
      if (ex instanceof DeferredGroupException) {
        ex = RowsWithErrorException.fromDeferredGroupException((DeferredGroupException) ex);
      }
      if (ex instanceof RowsWithErrorException) {
        RowsWithErrorException rwe = (RowsWithErrorException) ex;

        // If we encountered a failover, skip. See KUDU-568.
        if (rwe.areAllErrorsOfAlreadyPresentType(false)) {
          return;
        }

        System.out.println(rwe.toString());

        for (RowsWithErrorException.RowError error : rwe.getErrors()) {
          System.out.println(" " + error.getMessage());
        }
      } else {
        System.out.println("Got exception " + ex.toString());
      }
    }
  }

  // playground
  public static void main(String[] args) {
    /*if (args.length != 3) {
      System.out.println("Please specify a threadcount and operation count");
      System.exit(0);
    }*/

    final int keyspace = 10000; //120000000;

    final int threadcount = 1; //Integer.parseInt(args[0]);

    final int opcount = 10000; //Integer.parseInt(args[1]) / threadcount;

    List<Thread> allthreads = new ArrayList<Thread>(threadcount);

    for (int i = 0; i < threadcount; i++) {
      Thread t = new Thread() {
        public void run() {
          try {
            Random random = new Random();

            KuduYCSBClient cli = new KuduYCSBClient();

            Properties props = new Properties();
            props.setProperty(DEBUG_OPT, "true");
            //props.setProperty("masterAddress", "172.16.2.96");
            //props.setProperty("masterAddress", "172.21.0.225");
            props.setProperty("masterAddress", "192.168.1.32");
            props.setProperty("masterPort", "64000");
            cli.setProperties(props);

            cli.init();

            long accum = 0;

            HashSet<String> scanFields = new HashSet<String>();
            scanFields.add("field1");
            scanFields.add("field3");

            HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();

            for (int i = 0; i < opcount; i++) {
              int keynum = random.nextInt(keyspace);
              String key = "user" + keynum;
              long st = System.currentTimeMillis();
              int rescode;

              for (int f = 0; f < 11; f++) {
                values.put("field"+f, new StringByteIterator("some butch"));
              }
              //rescode = cli.insert("ycsb", key, values);
              //rescode = cli.delete("ycsb", key);

              HashMap<String, ByteIterator> getResult = new HashMap<String, ByteIterator>();
              Vector<HashMap<String, ByteIterator>> scanResult = new Vector<HashMap<String, ByteIterator>>();
              rescode = cli.read("ycsb", key, null, getResult);
              if (rescode == Ok) {
                values.clear();
                for (int f = 0; f < 11; f++) {
                  values.put("field"+f, new StringByteIterator("more! butch"));
                }
                cli.update("ycsb", key, values);
              }

              //rescode = cli.scan("table1", key, 20, scanFields, scanResult);

              long en = System.currentTimeMillis();

              accum += (en - st);

              if (rescode != Ok) {
                System.out.println("Error " + rescode + " for " + key);
              }

              if (i % 100 == 0) {
                System.out.println(i + " operations, average latency: " + (((double) accum) / ((double) i)));
              }
            }
            cli.cleanup();

            //System.out.println("Average latency: "+(((double)accum)/((double)opcount)));
            //System.out.println("Average get latency: "+(((double)cli.TotalGetTime)/((double)cli.TotalGetOps)));
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
      };
      allthreads.add(t);
    }

    long st = System.currentTimeMillis();
    for (Thread t : allthreads) {
      t.start();
    }

    for (Thread t : allthreads) {
      try {
        t.join(DEFAULT_SLEEP);
      } catch (InterruptedException e) {
      }
    }
    System.out.println("Going to cleanup");
    try {
      client.shutdown();
    } catch (Exception e) {
      e.printStackTrace();
    }
    long en = System.currentTimeMillis();

    System.out.println("Throughput: " + ((1000.0) * (((double) (opcount * threadcount)) / ((double) (en - st)))) + " ops/sec");

  }
}