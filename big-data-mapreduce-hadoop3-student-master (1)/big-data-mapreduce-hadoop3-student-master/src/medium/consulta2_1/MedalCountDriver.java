package medium.consulta2_1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

public class MedalCountDriver {
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        if (args.length != 2) {
            System.err.println("Usage: MedalCountDriver <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path outputPath = new Path(args[1]);

        // Check if output path exists and delete it if it does
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        Job job = Job.getInstance(conf, "Medal Count by Country and Year");
        job.setJarByClass(MedalCountDriver.class);

        job.setMapperClass(MedalCountMapper.class);
        job.setCombinerClass(MedalCountCombiner.class);
        job.setReducerClass(MedalCountReducer.class);

        job.setOutputKeyClass(CountryYearKey.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, outputPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}