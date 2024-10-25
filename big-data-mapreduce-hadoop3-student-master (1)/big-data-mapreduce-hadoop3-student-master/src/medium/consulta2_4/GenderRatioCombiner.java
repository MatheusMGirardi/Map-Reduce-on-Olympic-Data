package medium.consulta2_4;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class GenderRatioCombiner extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int maleCount = 0;
        int femaleCount = 0;

        for (IntWritable val : values) {
            if (val.get() == 1) maleCount++;
            else femaleCount++;
        }

        // Print the total counts
        System.out.println("Total males: " + maleCount);
        System.out.println("Total females: " + femaleCount);

        context.write(key, new IntWritable(maleCount));
        context.write(key, new IntWritable(femaleCount));
    }
}