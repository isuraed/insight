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

//import org.apache.log4j.Logger;

public class ProductIndexer {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "isura_ProductIndexer");

        job.setJarByClass(ProductIndexer.class);
        job.setMapperClass(IndexMapper.class);
        job.setReducerClass(IndexReducer.class);

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

    // The mapper builds the reverse index by tokenizing the title, removing stop words, and applying stemming.
    static class IndexMapper extends Mapper<LongWritable,Text,Text,Text> {
//        private static final Logger logger = Logger.getLogger(ReviewMapper.class.getName());

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] row = value.toString().split("\t");

            if (row.length != 2) {
                throw new IOException("Invalid row in input");
            }

            String productId = row[0];
            String title = row[1];
            title = title.toLowerCase();

            // Tokenize on all non alphanumeric characters except apostrophe.
            String[] titleWords = title.split("[^a-zA-Z0-9']+");

            // Create the reverse index.
            for (String word : titleWords) {
                context.write(new Text(word), new Text(productId));
            }
        }
    }

    // The reducer simply emits the key value because mapreduce will aggregate by keyword.
    static class IndexReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder productIds = new StringBuilder();
            for (Text val : values) {
                productIds.append(val.toString());
                productIds.append("\t");
            }

            context.write(key, new Text(productIds.toString()));
        }
    }
}
