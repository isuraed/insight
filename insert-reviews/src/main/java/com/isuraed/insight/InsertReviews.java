package com.isuraed.insight;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.util.Bytes;

import org.json.simple.JSONObject;

public class InsertReviews {

    // Insert records into HBase reviews table from all files in the specified HDFS folder. Input path is the full
    // hdfs path including hostname - i.e. hdfs://hostname:9000/path/to/files.
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.out.println("Usage: com.isuraed.insight.InsertReviews <hdfs_input_path> <hbase_table_name>");
            System.exit(1);
        }

        HTable htable = null;
        BufferedReader reader = null;
        try {
            FileSystem fs = FileSystem.get(new Configuration());
            FileStatus[] status = fs.listStatus(new Path(args[0]));

            htable = new HTable(HBaseConfiguration.create(), args[1]);

            for (int i = 0; i < status.length; i++) {
                reader = new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));

                String line = reader.readLine();
                while (line != null) {
                    String[] values = line.split("\t", -1);
                    String title = values[0];
                    String productId = values[1];
                    String userId = values[2];
                    String text = values[3];
                    long timestamp = Long.parseLong(values[4]);
                    float score = Float.parseFloat(values[5]);

                    JSONObject json = new JSONObject();
                    json.put("productId", productId);
                    json.put("userId", userId);
                    json.put("text", text);
                    json.put("timestamp", timestamp);
                    json.put("score", score);

                    if (title.length() > 0) {
                        byte[] rowKey = Bytes.toBytes(title);
                        byte[] colKey = Bytes.toBytes(timestamp + "_" + userId);
                        byte[] colValue = Bytes.toBytes(json.toJSONString());

                        Put put = new Put(rowKey);
                        put.add(Bytes.toBytes("cf1"), colKey, colValue);
                        htable.put(put);
                    }

                    line = reader.readLine();

                    if (i % 10000 == 0) {
                        System.out.println("Wrote record " + i);
                    }
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
