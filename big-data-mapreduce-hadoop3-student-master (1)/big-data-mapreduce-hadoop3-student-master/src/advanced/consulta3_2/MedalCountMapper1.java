package advanced.consulta3_2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MedalCountMapper1 extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final Text countryYearMedal = new Text();
    private final IntWritable one = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");
        String country = fields[6];  // Team
        String year = fields[9];  // Year
        String medal = fields[14];  // Medal

        if (!"NA".equals(medal)) {
            countryYearMedal.set(country + "," + year + "," + medal);
            context.write(countryYearMedal, one);
        }
    }
}
