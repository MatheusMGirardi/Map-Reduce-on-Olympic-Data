package advanced.consulta3_1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.DecimalFormat;

public class MaxAvgAgeReducer2 extends Reducer<Text, Text, Text, Text> {
    private static final DecimalFormat df = new DecimalFormat("#.00");

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String maxYear = "";
        double maxAvgAge = -1;

        for (Text val : values) {
            String[] yearAvg = val.toString().split(",");
            String year = yearAvg[0];
            double avgAge = Double.parseDouble(yearAvg[1]);

            if (avgAge > maxAvgAge) {
                maxAvgAge = avgAge;
                maxYear = year;
            }
        }

        context.write(key, new Text(maxYear + "," + df.format(maxAvgAge)));
    }
}