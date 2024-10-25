package advanced.consulta3_2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MedalCountMapper2 extends Mapper<LongWritable, Text, Text, Text> {
    private final Text countryYear = new Text();
    private final Text medalCount = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");
        String[] countryYearMedal = fields[0].split(",");
        String country = countryYearMedal[0];
        String year = countryYearMedal[1];
        String medalType = countryYearMedal[2];
        String count = fields[1];

        countryYear.set(country + "," + year);
        medalCount.set(medalType + "," + count);
        context.write(countryYear, medalCount);
    }
}
