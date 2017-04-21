package org.cs6240.utils;

/**
 * Created by dongxu on 4/20/17.
 */

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;


/**
 * Created by dongxu on 3/18/17.
 */
public class HBaseHelper {
    public static Configuration conf = null;
    private String tableName = null;
    private HTable table = null;

    static {
        conf = HBaseConfiguration.create();
        //conf.set("hbase.zookeeper.quorum", "127.0.0.1");
        //conf.set("hbase.zookeeper.property.clientPort", "2181");
    }

    public HBaseHelper(){

    }

    public HBaseHelper(String tableName) throws IOException{
        this.tableName = tableName;
        this.table = new HTable(conf, tableName);
    }

    public static Boolean createTable(String tableName, String family)
            throws Exception {

        HBaseAdmin admin = new HBaseAdmin(conf);
        if (admin.tableExists(tableName)) {
            return false;
        } else {
            // Add family then create table
            HTableDescriptor tableDesc = new HTableDescriptor(tableName);
            tableDesc.addFamily(new HColumnDescriptor(family));
            admin.createTable(tableDesc);
        }
        return true;
    }

    // Add a list of values to a row with given rowkey
    public void addRecordFields(String rowKey,
                                       String family, HashMap<String, Integer> header, String[] values) throws Exception {
        try {
            Put put = new Put(Bytes.toBytes(rowKey));

            // Iterate throught the header to get name of each field
            Iterator FieldIterator = header.entrySet().iterator();
            while (FieldIterator.hasNext()) {
                Map.Entry field = (Map.Entry)FieldIterator.next();
                // Here we use header to find the index of each field
                // and use the index to access the actual value in the list
                // The certain header is also the column name
                put.add(Bytes.toBytes(family), Bytes.toBytes(field.getKey().toString()),
                        Bytes.toBytes(values[(Integer)field.getValue()]));
            }
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void addRecordFieldsByHashMap(String rowKey,
                                       String family, HashMap<String, String> header) throws Exception {
        try {
            Put put = new Put(Bytes.toBytes(rowKey));

            // Iterate throught the header to get name of each field
            Iterator FieldIterator = header.entrySet().iterator();
            while (FieldIterator.hasNext()) {
                Map.Entry field = (Map.Entry)FieldIterator.next();
                // Here we use header to find the index of each field
                // and use the index to access the actual value in the list
                // The certain header is also the column name
                put.add(Bytes.toBytes(family), Bytes.toBytes(field.getKey().toString()),
                        Bytes.toBytes(field.getValue().toString()));
            }
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public Result getOneRecord (String rowKey) throws IOException{
        Get get = new Get(rowKey.getBytes());
        Result rs = table.get(get);
        return rs;
    }
}
