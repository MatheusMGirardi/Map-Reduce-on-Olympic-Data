package advanced.consulta3_1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AvgAgeMapper1 extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final Text teamYear = new Text();
    private final IntWritable age = new IntWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            String[] fields = value.toString().split(",");
            String team = fields[7];  // Team
            String yearStr = fields[9];  // Year
            String ageStr = fields[3];  // Age

            if (!"NA".equals(ageStr)) {
                int year = Integer.parseInt(yearStr);
                int athleteAge = Integer.parseInt(ageStr);

                teamYear.set(team + "," + year);
                age.set(athleteAge);
                context.write(teamYear, age);
            }
        } catch (Exception e) {
            System.err.println("Error processing record: " + value.toString());
            e.printStackTrace();
        }
    }
}
