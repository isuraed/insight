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

    // The mapper builds the reverse index by tokenizing the title. Don't remove stop words because titles are usually
    // short and the extra words helps search accuracy.
    static class IndexMapper extends Mapper<LongWritable,Text,Text,Text> {
        private static final Logger logger = Logger.getLogger(IndexMapper.class.getName());

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] row = value.toString().split("\t");

            if (row.length != 2) {
                logger.warn("Skipped row: invalid number of columns");
                return;
            }

            String productId = row[0];
            String title = row[1];
            title = title.toLowerCase();

            // Tokenize on all non alphanumeric characters and apostrophes.
            String[] titleWords = title.split("[^a-z0-9']+");

            // Create the reverse index.
            for (String word : titleWords) {
                if (word.length() > 0) {
                    context.write(new Text(word), new Text(productId));
                }
            }
        }
    }

    // The reducer simply emits the key value because mapreduce will aggregate by keyword.
    static class IndexReducer extends Reducer<Text, Text, Text, Text> {
        private static final Logger logger = Logger.getLogger(IndexReducer.class.getName());
        private static final int PRODUCT_LIMIT = 100000;

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder productIds = new StringBuilder();
            int productCount = 0;

            for (Text val : values) {
                // Must be a common word or weird case of many similar products having the same title. Ignore these
                // to avoid performance degradation during HBase import.
                if (productCount > PRODUCT_LIMIT) {
                    logger.info("Skipped keyword: " + key.toString() + " occurs in more than " + PRODUCT_LIMIT + " product titles");
                    return;
                }
                productIds.append(val.toString());
                productIds.append("\t");
                productCount++;
            }

            context.write(key, new Text(productIds.toString().trim()));
        }
    }
}
