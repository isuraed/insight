import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AveragesReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        List<TimeAndScore> scores = new ArrayList<TimeAndScore>();

        for (Text val : values) {
            String[] pair = val.toString().split(",");
            long time = Long.parseLong(pair[0]);
            int score = Integer.parseInt(pair[1]);
            scores.add(new TimeAndScore(time, score));
        }

        Collections.sort(scores);

        double total = 0.0;
        int n = 0;
        for (TimeAndScore pair : scores) {
            total += pair.getScore();
            n += 1;
            double average = total / n;

            Text outputValue = new Text();
            outputValue.set(pair.getTime() + "\t" + pair.getScore() + "\t" + average);
            context.write(key, outputValue);
        }
    }
}