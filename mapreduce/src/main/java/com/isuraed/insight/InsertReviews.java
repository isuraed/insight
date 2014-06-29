package com.isuraed.insight;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.util.Bytes;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

public class InsertReviews {

    // Insert records into HBase reviews table from all files in the specified HDFS folder. Input path is the full
    // hdfs path including hostname - i.e. hdfs://hostname:9000/path/to/files.
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.out.println("Usage: com.isuraed.insight.InsertReviews <hdfs_input_path> <hbase_table_name>");
            System.exit(1);
        }

        Logger logger = Logger.getLogger(InsertReviews.class.getName());

        Configuration conf = HBaseConfiguration.create();
        HBaseAdmin hadmin = new HBaseAdmin(conf);
        HTableDescriptor hdesc = new HTableDescriptor(args[1]);
        HColumnDescriptor hfamily = new HColumnDescriptor("cf1");
        hdesc.addFamily(hfamily);

        HTable htable = null;
        BufferedReader reader = null;
        try {
            FileSystem fs = FileSystem.get(new Configuration());
            FileStatus[] status = fs.listStatus(new Path(args[0]));

            if (hadmin.tableExists(args[1])) {
                hadmin.disableTable(args[1]);
                hadmin.deleteTable(args[1]);
                logger.info("Deleted existing table " + args[1]);
            }

            hadmin.createTable(hdesc);
            logger.info("Created table " + args[1]);

            htable = new HTable(conf, hdesc.getTableName());
            int rowCount = 0;

            for (int i = 0; i < status.length; i++) {
                reader = new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
                logger.info("Processing HDFS file " + status[i].getPath());

                String line = reader.readLine();
                while (line != null) {
                    String[] row = line.split("\t", -1);
                    byte[] rowKey = Bytes.toBytes(row[0]);
                    Object obj =JSONValue.parse(row[1]);
                    JSONArray jsonList = (JSONArray)obj;

                    Put put = new Put(rowKey);

                    for (int j = 0; j < jsonList.size(); j++) {
                        JSONObject jsonObj = (JSONObject)jsonList.get(j);

                        long timestamp = (Long)jsonObj.get("timestamp");
                        String userId = (String)jsonObj.get("userId");
                        byte[] colKey = Bytes.toBytes(timestamp + "_" + userId);
                        byte[] colValue = Bytes.toBytes(jsonObj.toJSONString());

                        put.add(hfamily.getName(), colKey, colValue);
                    }

                    htable.put(put);
                    rowCount++;

                    if (rowCount % 10000 == 0) {
                        logger.info(rowCount + " rows written");
                    }

                    line = reader.readLine();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        } finally {
            if (htable != null) htable.close();
            if (reader != null) reader.close();
        }
    }
}
