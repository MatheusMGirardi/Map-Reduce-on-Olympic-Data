package basic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

// Average Age By Gender
public class AverageAgeByGender {

    // Mapper
    public static class AverageAgeByGenderMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        private Text gender = new Text();
        private DoubleWritable ageWritable = new DoubleWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Ignorar o cabeçalho
            if (key.get() == 0 && value.toString().contains("ID")) {
                return;
            }

            String[] fields = parseCSVLine(value.toString());

            if (fields.length > 3) { // Verifica se há os campos 'Sex' e 'Age'
                String sex = fields[2];
                String ageStr = fields[3];
                if (!sex.isEmpty() && !ageStr.isEmpty() && !ageStr.equals("NA")) {
                    try {
                        double age = Double.parseDouble(ageStr);
                        gender.set(sex);
                        ageWritable.set(age);
                        context.write(gender, ageWritable);
                    } catch (NumberFormatException e) {
                        // Ignorar linhas com formato de idade inválido
                    }
                }
            }
        }

        private String[] parseCSVLine(String line) {
            // Implementação simples de parsing CSV, pode ser aprimorada
            return line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
        }
    }

    // Reducer
    public static class AverageAgeByGenderReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private DoubleWritable averageAge = new DoubleWritable();

        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0.0;
            int count = 0;
            for (DoubleWritable val : values) {
                sum += val.get();
                count++;
            }
            if (count > 0) {
                averageAge.set(sum / count);
                context.write(key, averageAge);
            }
        }
    }

    // Driver
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        if (args.length != 2) {
            System.err.println("Usage: AverageAgeByGender <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Average Age By Gender");
        job.setJarByClass(AverageAgeByGender.class);
        job.setMapperClass(AverageAgeByGenderMapper.class);
        job.setReducerClass(AverageAgeByGenderReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        // Verifica se a pasta de saída já existe e a remove
        Path outputPath = new Path(args[1]);
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true); // Deleta a pasta de saída existente
        }

        // Define input e output paths
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, outputPath);

        // Executa o job
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
