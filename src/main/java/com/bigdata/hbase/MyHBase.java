package com.bigdata.hbase;


import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Map;

public class MyHBase {

    private static Admin admin;
    private static Connection conn;

    public static void create(String table, String... colFamilies) throws IOException {
        TableName tableName = TableName.valueOf(table);

        if (admin.tableExists(tableName)) {
            System.out.println("Table already exists");
        } else {
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
            for (String colFamily: colFamilies) {
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(colFamily);
                hTableDescriptor.addFamily(hColumnDescriptor);
            }
            admin.createTable(hTableDescriptor);
            System.out.println("Table create successful");
        }
    }

    public static void insert(String table, int rowKey, Map<String, Map<String, String>> values) throws IOException {
        Put put = new Put(Bytes.toBytes(rowKey));
        values.forEach((key, value) -> {
            value.forEach((key1, value2) -> {
                put.addColumn(Bytes.toBytes(key), Bytes.toBytes(key1), Bytes.toBytes(value2)); // col2
            });
        });

        TableName tableName = TableName.valueOf(table);
        conn.getTable(tableName).put(put);
        System.out.println("Data insert success, rowKey = " + rowKey);
    }

    public static void get(String table, int rowKey) throws IOException {
        TableName tableName = TableName.valueOf(table);
        Get get = new Get(Bytes.toBytes(rowKey));
        if (!get.isCheckExistenceOnly()) {
            Result result = conn.getTable(tableName).get(get);
            for (Cell cell : result.rawCells()) {
                String colName = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                System.out.println("Data get success, colName: " + colName + ", value: " + value);
            }
        }
    }

    public static void delete(String table, int rowKey) throws IOException {
        TableName tableName = TableName.valueOf(table);

        Delete delete = new Delete(Bytes.toBytes(rowKey));
        conn.getTable(tableName).delete(delete);
        System.out.println("Delete Success");
    }

    public static void drop(String table) throws IOException {
        TableName tableName = TableName.valueOf(table);
        if (admin.tableExists(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            System.out.println("Table Delete Successful");
        } else {
            System.out.println("Table does not exist!");
        }
    }

    public static Configuration buildHBaseConfiguration(String host, String port) {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", host);
        configuration.set("hbase.zookeeper.property.clientPort", port);
        return configuration;
    }

    public static void main(String[] args) throws IOException {

        // 建立连接
        conn = ConnectionFactory.createConnection(
                buildHBaseConfiguration("emr-worker-2,emr-worker-1,emr-header-1", "2181"));
        admin = conn.getAdmin();

        String table = "chaichaoqun:student";

        // 建表
        create(table, "name", "info", "score");

        // 插入数据
        Map<String, Map<String, String>> mapTom = ImmutableMap.<String, Map<String, String>> builder()
                .put("name", ImmutableMap.of("name", "Tom"))
                .put("info", ImmutableMap.of("student_id", "20210000000001", "class", "1"))
                .put("score", ImmutableMap.of("understanding", "75", "programming", "82"))
                .build();
        insert(table, 1, mapTom);

        Map<String, Map<String, String>> mapJerry = ImmutableMap.<String, Map<String, String>> builder()
                .put("name", ImmutableMap.of("name", "Jerry"))
                .put("info", ImmutableMap.of("student_id", "20210000000002", "class", "1"))
                .put("score", ImmutableMap.of("understanding", "85", "programming", "67"))
                .build();
        insert(table, 2, mapJerry);

        Map<String, Map<String, String>> mapJack = ImmutableMap.<String, Map<String, String>> builder()
                .put("name", ImmutableMap.of("name", "Jack"))
                .put("info", ImmutableMap.of("student_id", "20210000000003", "class", "2"))
                .put("score", ImmutableMap.of("understanding", "80", "programming", "80"))
                .build();
        insert(table, 3, mapJack);

        Map<String, Map<String, String>> mapRose = ImmutableMap.<String, Map<String, String>> builder()
                .put("name", ImmutableMap.of("name", "Rose"))
                .put("info", ImmutableMap.of("student_id", "20210000000004", "class", "2"))
                .put("score", ImmutableMap.of("understanding", "60", "programming", "61"))
                .build();
        insert(table, 4, mapRose);

        Map<String, Map<String, String>> mapChaichaoqun = ImmutableMap.<String, Map<String, String>> builder()
                .put("name", ImmutableMap.of("name", "chaichaoqun"))
                .put("info", ImmutableMap.of("student_id", "G20210931010042", "class", "1"))
                .put("score", ImmutableMap.of("understanding", "100", "programming", "100"))
                .build();
        insert(table, 5, mapChaichaoqun);

        // 获取数据
        get(table, 1);
        get(table, 2);
        get(table, 3);
        get(table, 4);
        get(table, 5);

        // 删除数据
        delete(table, 1);
        delete(table, 2);

        // 删除表
        drop(table);

    }
}
