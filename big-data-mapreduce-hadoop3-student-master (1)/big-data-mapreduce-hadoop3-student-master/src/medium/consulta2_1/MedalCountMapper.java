package medium.consulta2_1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MedalCountMapper extends Mapper<Object, Text, CountryYearKey, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private CountryYearKey countryYearKey = new CountryYearKey();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");

        // Skip header line
        if (fields[0].equals("ID")) {
            return;
        }

        try {
            // Ensure all fields are present
            if (fields.length >= 15) {
                String country = fields[7].trim();
                String yearStr = fields[9].trim();
                String medal = fields[14].trim();

                // Log the yearStr to debug parsing issues
                System.out.println("yearStr: " + yearStr);

                // Check if year is parsable as an integer and if the medal field is valid
                if (!yearStr.isEmpty() && !medal.equals("NA")) {
                    int year = Integer.parseInt(yearStr);
                    countryYearKey.set(country, year);
                    context.write(countryYearKey, one);
                }
            }
        } catch (NumberFormatException e) {
            // Log the error and skip this line if parsing fails
            System.err.println("Skipping line due to parsing error: " + value.toString());
        }
    }
}