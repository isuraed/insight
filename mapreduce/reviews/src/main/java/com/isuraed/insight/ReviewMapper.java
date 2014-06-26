package com.isuraed.insight;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ReviewMapper extends Mapper<LongWritable,Text,Text,Text> {

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] rowValues= value.toString().split("\t");

        String productId = rowValues[0];
        String title = rowValues[1];
        String userId = rowValues[3];
        int score = (int) Double.parseDouble(rowValues[6]);
        long time = Long.parseLong(rowValues[7]);
        String text = rowValues[9];

        ReviewDetail detail = new ReviewDetail(productId, userId, text, time, score);
        context.write(new Text(title), new Text(detail.toString()));
    }
}
