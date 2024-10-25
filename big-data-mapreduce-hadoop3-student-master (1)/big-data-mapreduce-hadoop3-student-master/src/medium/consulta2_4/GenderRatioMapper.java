package medium.consulta2_4;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class GenderRatioMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
    private IntWritable yearKey = new IntWritable();
    private IntWritable genderValue = new IntWritable();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");

        if (fields.length > 9 && fields.length > 2) { // Ensure fields exist
            String yearStr = fields[9].trim();
            String gender = fields[2].trim();

            try {
                int year = Integer.parseInt(yearStr);
                yearKey.set(year);

                // Emit 1 for male, 0 for female
                if (gender.equals("M")) {
                    genderValue.set(1);
                } else if (gender.equals("F")) {
                    genderValue.set(0);
                }
                context.write(yearKey, genderValue);
            } catch (NumberFormatException e) {
                System.err.println("Skipping line due to parsing error: " + value.toString());
            }
        }
    }
}