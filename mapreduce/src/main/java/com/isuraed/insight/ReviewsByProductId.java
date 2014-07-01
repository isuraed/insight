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

public class ReviewsByProductId {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "isura_ReviewsByProductId");

        job.setJarByClass(ReviewsByProductId.class);
        job.setMapperClass(ReviewsMapper.class);
        job.setReducerClass(ReviewsReducer.class);

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

    // The mapper skips bad records, cleans the field values, and emits the required fields from the raw data.
    static class ReviewsMapper extends Mapper<LongWritable,Text,Text,Text> {
        private static final Logger logger = Logger.getLogger(ReviewsMapper.class.getName());

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] rowValues = value.toString().split("\t", -1);

            if (rowValues.length != 10) {
                logger.warn("Skipped record: Wrong number of columns. Expected=10, Receieved=" + rowValues.length);
                return;
            }

            String productId = rowValues[0];
            String title = rowValues[1];
            String userId = rowValues[3];
            String profileName = rowValues[4];
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
                logger.info("Skipped record: unknown username");
                return;
            }

            // Escape quotes because strings are stored in json later.
            title = title.replace("\"", "\\\"");
            text = text.replace("\"", "\\\"");

            StringBuilder outputValues = new StringBuilder();
            outputValues.append(productId + "\t");
            outputValues.append(title + "\t");
            outputValues.append(userId + "\t");
            outputValues.append(profileName + "\t");
            outputValues.append(score + "\t");
            outputValues.append(time + "\t");
            outputValues.append(text);

            context.write(new Text(productId), new Text(outputValues.toString()));
        }
    }

    // The reducer is responsible for packing the reviews for each productId into a JSON list.
    static class ReviewsReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            JSONArray jsonList = new JSONArray();

            for (Text val : values) {
                String[] rowValues = val.toString().split("\t", -1);

                String productId = rowValues[0];
                String title = rowValues[1];
                String userId = rowValues[2];
                String profileName = rowValues[3];
                float score = Float.parseFloat(rowValues[4]);
                long timestamp = Long.parseLong(rowValues[5]);
                String text = rowValues[6];

                JSONObject jsonObj = new JSONObject();
                jsonObj.put("productId", productId);
                jsonObj.put("title", title);
                jsonObj.put("userId", userId);
                jsonObj.put("profileName", profileName);
                jsonObj.put("score", score);
                jsonObj.put("timestamp", timestamp);
                jsonObj.put("text", text);

                jsonList.add(jsonObj);
            }

            context.write(key, new Text(jsonList.toJSONString()));
        }
    }
}
