import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AveragesMapper extends Mapper<Text,Text,Text,Text> {

    public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        String[] row = value.toString().split("\t");

        String productId = row[0];
        long time = Long.parseLong(row[7]);
        long score = Long.parseLong(row[6]);
        TimeAndScore pair = new TimeAndScore(time, score);

        context.write(new Text(productId), new Text(pair.toString()));
    }
}