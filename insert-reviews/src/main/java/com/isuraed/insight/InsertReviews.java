package com.isuraed.insight;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.util.Bytes;

import org.json.simple.JSONObject;

public class InsertReviews {

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.out.println("Usage: com.isuraed.insight.InsertReviews <hdfs_input_path>");
            System.exit(1);
        }

        String hdfsFile = args[0];
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(hdfsFile), conf);
        Path hdfsPath = new Path(hdfsFile);

        if (!fs.exists(hdfsPath)) {
            System.out.println("File " + hdfsFile + " does not exist.");
            System.exit(1);
        }

        Configuration hconf = HBaseConfiguration.create();
        HTable htable = new HTable(hconf, "isura_reviews");
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(hdfsPath)));

        String line = br.readLine();
        while (line != null) {
            String[] values = line.split("\t");
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

            byte[] rowKey = Bytes.toBytes(title);
            byte[] colKey = Bytes.toBytes(timestamp + "_" + userId);
            byte[] colValue = Bytes.toBytes(json.toJSONString());

            Put put = new Put(rowKey);
            put.add(Bytes.toBytes("cf1"), colKey, colValue);
            htable.put(put);

            line = br.readLine();
        }

        br.close();
        htable.close();
    }
}
