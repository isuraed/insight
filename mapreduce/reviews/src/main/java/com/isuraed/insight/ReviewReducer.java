package com.isuraed.insight;

import java.io.InterruptedIOException;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

// The reducer is responsible for packing the reviews for each product title (key) into a long json list.
public class ReviewReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        JSONArray jsonList = new JSONArray();

        for (Text val : values) {
            String[] rowValues = val.toString().split("\t", -1);

            String productId = rowValues[0];
            String userId = rowValues[1];
            String text = rowValues[2];
            long timestamp = Long.parseLong(rowValues[3]);
            float score = Float.parseFloat(rowValues[4]);

            JSONObject jsonObj = new JSONObject();
            jsonObj.put("productId", productId);
            jsonObj.put("userId", userId);
            jsonObj.put("text", text);
            jsonObj.put("timestamp", timestamp);
            jsonObj.put("score", score);

            jsonList.add(jsonObj);
        }

        context.write(key, new Text(jsonList.toJSONString()));
    }
}