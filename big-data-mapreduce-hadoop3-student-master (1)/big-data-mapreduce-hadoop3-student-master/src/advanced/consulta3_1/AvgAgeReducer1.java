package advanced.consulta3_1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.DecimalFormat;

public class AvgAgeReducer1 extends Reducer<Text, IntWritable, Text, Text> {
    private static final DecimalFormat df = new DecimalFormat("#.00");

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        int count = 0;

        for (IntWritable val : values) {
            sum += val.get();
            count++;
        }

        double avgAge = (double) sum / count;
        context.write(key, new Text(df.format(avgAge) + "," + count));
    }
}