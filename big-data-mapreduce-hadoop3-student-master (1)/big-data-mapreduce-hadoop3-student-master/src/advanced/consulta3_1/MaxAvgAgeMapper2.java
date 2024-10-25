package advanced.consulta3_1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MaxAvgAgeMapper2 extends Mapper<LongWritable, Text, Text, Text> {
    private final Text team = new Text();
    private final Text yearAvgAge = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            String[] fields = value.toString().split("\t");
            String[] teamYear = fields[0].split(",");
            String teamName = teamYear[0];
            String year = teamYear[1];
            String avgAge = fields[1].split(",")[0];

            team.set(teamName);
            yearAvgAge.set(year + "," + avgAge);
            context.write(team, yearAvgAge);

            // Log the output key-value pair
            System.out.println("Mapper Output: " + team + " -> " + yearAvgAge);
        } catch (Exception e) {
            System.err.println("Error processing record: " + value.toString());
            e.printStackTrace();
        }
    }
}