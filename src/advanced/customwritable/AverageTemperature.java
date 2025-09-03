package advanced.customwritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class AverageTemperature {

    public static void main(String args[]) throws IOException,
            ClassNotFoundException,
            InterruptedException {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path(files[0]);

        // arquivo de saida
        Path output = new Path(files[1]);

        // criacao do job e seu nome
        Job j = new Job(c, "media");

        //Precisamos fazer 4 configurações adicionais
        //1 - Registrar as classes
        //2 - Definir os tipos de saída (chave, valor) do Map e do Reduce
        //3 - Cadastrar os arquivos de entrada e saída
        //4 - Lançar o exit

        //1
        j.setJarByClass(AverageTemperature.class);
        j.setMapperClass(MapForAverage.class);
        j.setReducerClass(ReduceForAverage.class);

        //2
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(FireAvgTempWritable.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(FloatWritable.class);

        //3
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        //4
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    //7,5,mar,fri,86.2,26.2,94.3,5.1,8.2,51,6.7,0,0
    public static class MapForAverage extends Mapper<LongWritable, Text, Text, FireAvgTempWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            String linha = value.toString();
            String[] col = linha.split(",");
            Float temp = Float.parseFloat(col[8]);//Temperatura
            int freq = 1;
            //valores serão compostos (temp,freq)
            //tupla de saida: x, (temp,freq)
            con.write(new Text("x"), new FireAvgTempWritable(temp, freq));
        }
    }

//    public static class CombineForAverage extends Reducer<Text, FireAvgTempWritable, Text, FireAvgTempWritable>{
//        public void reduce(Text key, Iterable<FireAvgTempWritable> values, Context con)
//                throws IOException, InterruptedException {
//        }
//    }


    public static class ReduceForAverage extends Reducer<Text, FireAvgTempWritable, Text, FloatWritable> {
        public void reduce(Text key, Iterable<FireAvgTempWritable> values, Context con)
                throws IOException, InterruptedException {
            float somaTemp = 0;
            int somaFreq = 0;
            for (FireAvgTempWritable v : values) {
                somaTemp += v.getTemperature();
                somaFreq += v.getFrequency();
            }
            float avg = somaTemp / somaFreq;
            con.write(new Text("Media:"), new FloatWritable(avg));
        }
    }

}
