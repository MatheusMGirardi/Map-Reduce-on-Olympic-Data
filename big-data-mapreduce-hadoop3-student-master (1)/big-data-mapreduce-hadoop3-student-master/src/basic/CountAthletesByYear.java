package basic;


import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

// Count Athletes by Year
public class CountAthletesByYear {

    // Mapper
    public static class CountAthletesByYearMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Text year = new Text();
        private final static IntWritable one = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Ignorar o cabeçalho
            if (key.get() == 0 && value.toString().contains("ID")) {
                return;
            }

            String[] fields = parseCSVLine(value.toString());

            if (fields.length > 10) { // Verifica se há o campo 'Year'
                String yearStr = fields[9];
                if (!yearStr.isEmpty()) {
                    year.set(yearStr);
                    context.write(year, one);
                }
            }
        }

        private String[] parseCSVLine(String line) {
            // Implementação simples de parsing CSV, pode ser aprimorada
            return line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
        }
    }

    // Reducer
    public static class CountAthletesByYearReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable totalAthletes = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            totalAthletes.set(sum);
            context.write(key, totalAthletes);
        }
    }

    // Driver
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        if (args.length != 2) {
            System.err.println("Usage: CountAthletesByYear <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Count Athletes By Year");
        job.setJarByClass(CountAthletesByYear.class);
        job.setMapperClass(CountAthletesByYearMapper.class);
        job.setReducerClass(CountAthletesByYearReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Verifica se a pasta de saída já existe e a remove
        Path outputPath = new Path(args[1]);
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true); // Deleta a pasta de saída existente
        }

        // Define input and output paths
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, outputPath);

        // Executa o job
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
