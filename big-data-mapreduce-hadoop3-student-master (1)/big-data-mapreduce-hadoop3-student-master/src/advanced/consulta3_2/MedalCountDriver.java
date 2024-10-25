package advanced.consulta3_2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

public class MedalCountDriver {
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure(); // log4j
        if (args.length != 3) {
            System.err.println("Usage: MedalCountDriver <input path> <temp path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        // Clean up the output directories if they already exist
        Path outputPath1 = new Path(args[1]);
        Path outputPath2 = new Path(args[2]);
        if (fs.exists(outputPath1)) {
            fs.delete(outputPath1, true);
        }
        if (fs.exists(outputPath2)) {
            fs.delete(outputPath2, true);
        }

        // Job 1
        Job job1 = Job.getInstance(conf, "medal count per country and year");
        job1.setJarByClass(MedalCountDriver.class);
        job1.setMapperClass(MedalCountMapper1.class);
        job1.setReducerClass(MedalCountReducer1.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        if (job1.waitForCompletion(true)) {
            // Job 2
            Job job2 = Job.getInstance(conf, "aggregate medal count per country and year");
            job2.setJarByClass(MedalCountDriver.class);
            job2.setMapperClass(MedalCountMapper2.class);
            job2.setReducerClass(MedalCountReducer2.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job2, new Path(args[1]));
            FileOutputFormat.setOutputPath(job2, new Path(args[2]));

            System.exit(job2.waitForCompletion(true) ? 0 : 1);
        } else {
            System.exit(1);
        }
    }
}
