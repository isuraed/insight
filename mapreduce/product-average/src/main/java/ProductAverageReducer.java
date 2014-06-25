import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ProductAverageReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        List<ReviewDetail> details = new ArrayList<ReviewDetail>();

        for (Text val : values) {
            String[] data = val.toString().split(",");
            String userId = data[0];
            long time = Long.parseLong(data[1]);
            int score = Integer.parseInt(data[2]);
            details.add(new ReviewDetail(userId, time, score));
        }

        // Review details are sorted by timestamp.
        Collections.sort(details);

        // Calculate the average rating at each time period.
        double runningTotal = 0.0;
        int n = 0;
        for (ReviewDetail d : details) {
            runningTotal += d.getScore();
            n += 1;
            double averageScore = runningTotal / n;

            Text outputValue = new Text();
            outputValue.set(d.getUserId() + "\t" + d.getTime() + "\t" + d.getScore() + "\t" + averageScore);
            context.write(key, outputValue);
        }
    }
}