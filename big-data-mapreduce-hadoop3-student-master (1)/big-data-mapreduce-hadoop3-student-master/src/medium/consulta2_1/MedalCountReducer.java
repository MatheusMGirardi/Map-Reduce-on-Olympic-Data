package medium.consulta2_1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MedalCountReducer extends Reducer<CountryYearKey, IntWritable, CountryYearKey, IntWritable> {
    public void reduce(CountryYearKey key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        context.write(key, new IntWritable(sum));
    }
}