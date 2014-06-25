import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ProductAverageMapper extends Mapper<LongWritable,Text,Text,Text> {

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] row = value.toString().split("\t");

        String productId = row[0];
        String userId = row[3];
        int score = (int) Double.parseDouble(row[6]);
        long time = Long.parseLong(row[7]);
        ReviewDetail detail = new ReviewDetail(userId, time, score);

        context.write(new Text(productId), new Text(detail.toString()));
    }
}