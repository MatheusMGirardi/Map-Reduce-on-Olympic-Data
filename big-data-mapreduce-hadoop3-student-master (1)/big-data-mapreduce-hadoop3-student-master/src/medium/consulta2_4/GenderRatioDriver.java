package medium.consulta2_4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

public class GenderRatioDriver {
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure(); // log4j
        if (args.length != 2) {
            System.err.println("Usage: GenderRatioDriver <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        // Delete output folder if it already exists
        Path outputPath = new Path(args[1]);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        Job job = Job.getInstance(conf, "Gender Ratio by Year");
        job.setJarByClass(GenderRatioDriver.class);

        job.setMapperClass(GenderRatioMapper.class);
        job.setCombinerClass(GenderRatioCombiner.class);
        job.setReducerClass(GenderRatioReducer.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, outputPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}