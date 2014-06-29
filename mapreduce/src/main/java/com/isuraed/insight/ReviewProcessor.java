package com.isuraed.insight;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.log4j.Logger;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

public class ReviewProcessor {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "isura_ReviewProcessor");

        job.setJarByClass(ReviewProcessor.class);
        job.setMapperClass(ReviewMapper.class);
        job.setReducerClass(ReviewReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }

    // The mapper is responsible for projecting the required columns and skipping bad records.
    static class ReviewMapper extends Mapper<LongWritable,Text,Text,Text> {
        private static final Logger logger = Logger.getLogger(ReviewMapper.class.getName());

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] rowValues = key.toString().split("\t", -1);

            if (rowValues.length != 10) {
                logger.warn("Skipped record: Wrong number of columns. Expected=10, Receieved=" + rowValues.length);
                return;
            }

            String productId = rowValues[0];
            String title = rowValues[1];
            String userId = rowValues[3];
            int score = (int) Double.parseDouble(rowValues[6]);
            long time = Long.parseLong(rowValues[7]);
            String text = rowValues[9];

            if (title.equals("")) {
                logger.info("Skipped record: Blank title");
                return;
            }
            if (text.equals("")) {
                logger.info("Skipped record: Blank text");
                return;
            }
            if (userId.equalsIgnoreCase("unknown")) {
                // Don't log these because unknown is very common.
                return;
            }

            // Escape quotes because strings are stored in json later.
            title = title.replace("\"", "\\\"");
            text = text.replace("\"", "\\\"");

            // Store the key as lowercase for easier searching.
            String titleKey = title.toLowerCase();

            // StringBuilder has big performance improvement because the text field can be huge.
            StringBuilder outputValues = new StringBuilder();
            outputValues.append(title + "\t");
            outputValues.append(productId + "\t");
            outputValues.append(userId + "\t");
            outputValues.append(text);
            outputValues.append("\t");
            outputValues.append(time);
            outputValues.append("\t");
            outputValues.append(score);

            context.write(new Text(titleKey), new Text(outputValues.toString()));
        }
    }

    // The reducer is responsible for packing the reviews for each product title (key) into a long json list.
    static class ReviewReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            JSONArray jsonList = new JSONArray();

            for (Text val : values) {
                String[] rowValues = val.toString().split("\t", -1);

                String title = rowValues[0];
                String productId = rowValues[1];
                String userId = rowValues[2];
                String text = rowValues[3];
                long timestamp = Long.parseLong(rowValues[4]);
                float score = Float.parseFloat(rowValues[5]);

                JSONObject jsonObj = new JSONObject();
                jsonObj.put("title", title);
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
}
