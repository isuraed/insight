import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AveragesMapper extends Mapper<LongWritable,Text,Text,Text> {

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] row = value.toString().split("\t");

        String productId = row[0];
        long time = Long.parseLong(row[7]) * 1000;
        int score = (int) Double.parseDouble(row[6]);
        TimeAndScore pair = new TimeAndScore(time, score);

        context.write(new Text(productId), new Text(pair.toString()));
    }
}