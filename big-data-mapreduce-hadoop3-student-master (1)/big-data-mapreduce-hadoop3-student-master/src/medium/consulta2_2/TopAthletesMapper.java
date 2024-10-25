package medium.consulta2_2;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TopAthletesMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text athleteName = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");
        String athlete = fields[1];
        String medal = fields[14];

        if (!medal.equals("NA")) { // Se o atleta tem medalha
            athleteName.set(athlete);
            context.write(athleteName, one);
        }
    }
}





