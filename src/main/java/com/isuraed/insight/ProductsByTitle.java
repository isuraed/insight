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

public class ProductsByTitle {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "isura_ReviewsByProductTitle");

        job.setJarByClass(ProductsByTitle.class);
        job.setMapperClass(ProductsMapper.class);
        job.setReducerClass(ProductsReducer.class);

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

    // Earlier in the pipeline we already calculated the title for each productId. The mapper simply reverses the key and value.
    static class ProductsMapper extends Mapper<LongWritable,Text,Text,Text> {
        private static final Logger logger = Logger.getLogger(ProductsMapper.class.getName());

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] row = value.toString().split("\t", -1);
            String productId = row[0].toString();
            String title = row[1].toString();

            if (productId.equals("")) {
                logger.warn("Skipped record: productId was blank");
                return;
            }

            if (title.equals("")) {
                logger.warn("Skipped record: Title was blank");
                return;
            }

            // Escape quotes because strings are stored in json later.
            title = title.replace("\"", "\\\"");
            title = title.toLowerCase();

            context.write(new Text(title), new Text(productId));
        }
    }

    static class ProductsReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder productIds = new StringBuilder();

            for (Text val : values) {
                productIds.append(val);
                productIds.append("\t");
            }

            context.write(key, new Text(productIds.toString().trim()));
        }
    }
}
