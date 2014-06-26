package com.isuraed.insight;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TransformReviewsMapper extends Mapper<LongWritable,Text,Text,Text> {

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] rowValues = value.toString().split("\t");

        String productId = rowValues[0];
        String title = rowValues[1];
        String userId = rowValues[3];
        int score = (int) Double.parseDouble(rowValues[6]);
        long time = Long.parseLong(rowValues[7]);
        String text = rowValues[9];

        if (title == "" || userId == "unknown") {
            return;
        }

        StringBuilder outputValues = new StringBuilder();
        outputValues.append(productId);
        outputValues.append("\t");
        outputValues.append(userId);
        outputValues.append("\t");
        outputValues.append(text);
        outputValues.append("\t");
        outputValues.append(time);
        outputValues.append("\t");
        outputValues.append(score);

        context.write(new Text(title), new Text(outputValues.toString()));
    }
}
