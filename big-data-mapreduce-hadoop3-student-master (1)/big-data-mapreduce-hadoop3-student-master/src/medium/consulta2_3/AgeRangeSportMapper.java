package medium.consulta2_3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AgeRangeSportMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text ageRangeSport = new Text();
    private boolean isHeader = true;

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Skip the header line
        if (isHeader) {
            isHeader = false;
            return;
        }

        String[] fields = value.toString().split(",");
        try {
            int age = Integer.parseInt(fields[3]);
            String sport = fields[12];

            String ageRange = "";
            if (age >= 20 && age <= 30) ageRange = "20-30";
            else if (age >= 31 && age <= 40) ageRange = "31-40";
            else ageRange = "41+";

            ageRangeSport.set(ageRange + "," + sport);
            context.write(ageRangeSport, one);
        } catch (NumberFormatException e) {
            // Log the error and skip this record
            System.err.println("Skipping record due to invalid age: " + fields[3]);
        }
    }
}