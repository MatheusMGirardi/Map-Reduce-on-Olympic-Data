package medium.consulta2_4;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class GenderRatioReducer extends Reducer<IntWritable, IntWritable, IntWritable, Text> {
    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int maleCount = 0;
        int femaleCount = 0;
        boolean isMale = true;

        for (IntWritable val : values) {
            if (isMale) {
                maleCount += val.get();
                isMale = false;
            } else {
                femaleCount += val.get();
                isMale = true;
            }
        }

        // Calculate the ratios
        double maleRatio = (maleCount + femaleCount > 0) ? (double) maleCount / (maleCount + femaleCount) : 0.0;
        double femaleRatio = 1.0 - maleRatio;

        // Format the ratios to two decimal places
        String formattedMaleRatio = String.format("%.2f", maleRatio);
        String formattedFemaleRatio = String.format("%.2f", femaleRatio);

        context.write(key, new Text("Male: " + formattedMaleRatio + " Female: " + formattedFemaleRatio));
    }
}