package org.cs6240;

/**
 * Created by dongxu on 4/5/17.
 */
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class Test {

    public static void main(String[] arg) throws IOException {
        Configuration config = HBaseConfiguration.create();
//        config.set("hbase.zookeeper.quorum", "138.68.254.75");
//        config.set("hbase.zookeeper.property.clientPort", "2181");

        HTable testTable = new HTable(config, "test");

        for (int i = 0; i < 100; i++) {
            byte[] family = Bytes.toBytes("cf");
            byte[] qual = Bytes.toBytes("a");

            Scan scan = new Scan();
            scan.addColumn(family, qual);
            ResultScanner rs = testTable.getScanner(scan);
            for (Result r = rs.next(); r != null; r = rs.next()) {
                byte[] valueObj = r.getValue(family, qual);
                String value = new String(valueObj);
                System.out.println(value);
            }
        }

        testTable.close();
    }
}