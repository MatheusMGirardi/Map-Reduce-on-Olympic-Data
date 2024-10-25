package medium.consulta2_2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class TopAthletesReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private TreeMap<Integer, List<Text>> topAthletes = new TreeMap<>(Collections.reverseOrder());

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }

        // Armazenar na TreeMap para ordenar
        topAthletes.putIfAbsent(sum, new ArrayList<>());
        topAthletes.get(sum).add(new Text(key));

        // Manter apenas os 3 primeiros
        if (topAthletes.size() > 3) {
            topAthletes.pollLastEntry();
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Map.Entry<Integer, List<Text>> entry : topAthletes.entrySet()) {
            for (Text athlete : entry.getValue()) {
                context.write(athlete, new IntWritable(entry.getKey()));
            }
        }
    }
}