package advanced.consulta3_2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;

public class MedalCountReducer2 extends Reducer<Text, Text, Text, Text> {
    private static final DecimalFormat df = new DecimalFormat("#.00");

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Map<String, Integer> medalCounts = new HashMap<>();
        int totalMedals = 0;

        for (Text val : values) {
            String[] medalCount = val.toString().split(",");
            String medalType = medalCount[0];
            int count = Integer.parseInt(medalCount[1]);

            medalCounts.put(medalType, medalCounts.getOrDefault(medalType, 0) + count);
            totalMedals += count;
        }

        String maxMedalType = "";
        int maxCount = 0;
        for (Map.Entry<String, Integer> entry : medalCounts.entrySet()) {
            if (entry.getValue() > maxCount) {
                maxMedalType = entry.getKey();
                maxCount = entry.getValue();
            }
        }

        double maxPercentage = (double) maxCount / totalMedals * 100;
        context.write(key, new Text(maxMedalType + "," + maxCount + "," + df.format(maxPercentage)));
    }
}
