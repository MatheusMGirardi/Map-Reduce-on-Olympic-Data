package medium.consulta2_2;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.BasicConfigurator;

public class TopAthletesDriver {
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure(); //log4j
        if (args.length != 2) {
            System.err.println("Usage: TopAthletesDriver <input path> <output path>");
            System.exit(-1);
        }

        Job job = Job.getInstance(new Configuration(), "Top 3 Athletes with Most Medals");
        job.setJarByClass(TopAthletesDriver.class);

        job.setMapperClass(TopAthletesMapper.class);
        job.setCombinerClass(TopAthletesCombiner.class);
        job.setReducerClass(TopAthletesReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}