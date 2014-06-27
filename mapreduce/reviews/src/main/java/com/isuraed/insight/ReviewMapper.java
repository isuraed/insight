package com.isuraed.insight;

import java.io.IOException;
import java.util.logging.Logger;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.log4j.Logger;

// The mapper is responsible for projecting the required columns and skipping bad records.
public class ReviewMapper extends Mapper<LongWritable,Text,Text,Text> {
    private static final Logger logger = Logger.getLogger(ReviewMapper.class.getName());

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // -1 handles the case when the \t delimiter is the last character in the line (empty text column).
        String[] rowValues = value.toString().split("\t", -1);

        if (rowValues.length != 10) {
            logger.warning("Skipped record: Wrong number of columns. Expected=10, Receieved=" + rowValues.length + ".");
            return;
        }

        String productId = rowValues[0];
        String title = rowValues[1];
        String userId = rowValues[3];
        int score = (int) Double.parseDouble(rowValues[6]);
        long time = Long.parseLong(rowValues[7]);
        String text = rowValues[9];

        if (title.equals("")) return;
        if (text.equals("")) return;
        if (userId.equalsIgnoreCase("unknown")) return;

        // Escape quotes because strings are stored in json later.
        title = title.replace("\"", "\\\"");
        text = text.replace("\"", "\\\"");

        StringBuilder outputValues = new StringBuilder();
        outputValues.append(productId + "\t");
        outputValues.append(userId + "\t");
        outputValues.append(text);
        outputValues.append("\t");
        outputValues.append(time);
        outputValues.append("\t");
        outputValues.append(score);

        context.write(new Text(title), new Text(outputValues.toString()));
    }
}
