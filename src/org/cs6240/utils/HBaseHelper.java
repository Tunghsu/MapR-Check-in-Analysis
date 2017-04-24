package org.cs6240.utils;

/**
 * Created by dongxu on 4/20/17.
 */

import java.io.IOException;
import java.io.Serializable;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.commons.lang.SerializationUtils;


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

    public void addRecordSingleField(String rowKey,
                                         String family, String field, String value) throws Exception {
        try {
            Put put = new Put(SerializationUtils.serialize(rowKey));
            put.add(Bytes.toBytes(family), Bytes.toBytes(field), Bytes.toBytes(value));
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void addRecordSerilzibleField(Serializable rowKey,
                                     String family, Serializable field, Serializable value) throws Exception {
        try {
            Put put = new Put(SerializationUtils.serialize(rowKey));
            put.add(Bytes.toBytes(family), SerializationUtils.serialize(field), SerializationUtils.serialize(value));
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public ArrayList<Result> getValueFilteredSerilzibleFields(Serializable value, Serializable field) throws IOException{
        ArrayList<Result> rs = new ArrayList<>();
        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes("data"), SerializationUtils.serialize(field));
        Filter filter = new SingleColumnValueFilter(Bytes.toBytes("data"), SerializationUtils.serialize(field),
                CompareFilter.CompareOp.EQUAL, SerializationUtils.serialize(value));
        scan.setFilter(filter);

        ResultScanner scanner = table.getScanner(scan);
        for (Result result = scanner.next(); result != null; result = scanner.next()){
            rs.add(result);
        }
        return rs;
    }

    public Result getOneRecord (String rowKey) throws IOException{
        Get get = new Get(rowKey.getBytes());
        Result rs = table.get(get);
        return rs;
    }

    public ArrayList<Result> getValueFilteredRecords(String value, String field) throws IOException{
        ArrayList<Result> rs = new ArrayList<>();
        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes("data"), Bytes.toBytes(field));
        Filter filter = new SingleColumnValueFilter(Bytes.toBytes("data"), Bytes.toBytes(field),
                CompareFilter.CompareOp.EQUAL, Bytes.toBytes(value));
        scan.setFilter(filter);

        ResultScanner scanner = table.getScanner(scan);
        for (Result result = scanner.next(); result != null; result = scanner.next()){
            rs.add(result);
        }
        return rs;
    }

    public ArrayList<Result> getMultiValuesFilteredRecords(HashMap<String, Serializable> map) throws IOException{
        ArrayList<Result> rs = new ArrayList<>();
        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes("data"), Bytes.toBytes("city"));
        FilterList filterList = new FilterList();
        for (Map.Entry e: map.entrySet()) {
            Filter filter = new SingleColumnValueFilter(Bytes.toBytes("data"), Bytes.toBytes((String)e.getKey()),
                    CompareFilter.CompareOp.EQUAL, SerializationUtils.serialize((Serializable) e.getValue()));
            filterList.addFilter(filter);
        }
        scan.setFilter(filterList);

        ResultScanner scanner = table.getScanner(scan);
        for (Result result = scanner.next(); result != null; result = scanner.next()){
            rs.add(result);
        }
        return rs;
    }

    // http://tec.5lulu.com/detail/109k1n1h941ar8yc3.html
    public long rowCount() {
        long rowCount = 0;
        try {
            Scan scan = new Scan();
            ResultScanner resultScanner = table.getScanner(scan);
            for (Result result : resultScanner) {
                rowCount += result.size();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return rowCount;
    }

    public void removeSerializedRow(java.io.Serializable rowKey) throws IOException {
        Delete delete = new Delete(SerializationUtils.serialize(rowKey));

        table.delete(delete);
    }

    public void removeRow(String rowKey) throws IOException {
        Delete delete = new Delete(Bytes.toBytes(rowKey));

        table.delete(delete);
    }

}
