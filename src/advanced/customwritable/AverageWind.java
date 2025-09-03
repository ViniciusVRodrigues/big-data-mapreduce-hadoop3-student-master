package advanced.customwritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class AverageWind {

    public static void main(String args[]) throws IOException,
            ClassNotFoundException,
            InterruptedException {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path("in/forestfireinput.csv");

        // arquivo de saida
        Path output = new Path("output/saida_combiner1");

        // criacao do job e seu nome
        Job j = new Job(c, "mediawind");



        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        System.exit(j.waitForCompletion(true)?0:1);

    }


    public static class MapForAverage extends Mapper<LongWritable, Text, Text, FireAvgWindWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

        }
    }

    public static class CombineForAverage extends Reducer<Text, FireAvgWindWritable, Text, FireAvgWindWritable> {
        public void reduce(Text key, Iterable<FireAvgWindWritable> values, Context con)
                throws IOException, InterruptedException {

        }
    }


    public static class ReduceForAverage extends Reducer<Text, FireAvgWindWritable, Text, FloatWritable> {
        public void reduce(Text key, Iterable<FireAvgWindWritable> values, Context con)
                throws IOException, InterruptedException {


        }
    }

}
