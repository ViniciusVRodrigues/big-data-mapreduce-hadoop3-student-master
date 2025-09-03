package basic;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;


public class WordCount {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path(files[0]);

        // arquivo de saida
        Path output = new Path(files[1]);

        // criacao do job e seu nome
        Job j = new Job(c, "wordcount");

        //Precisamos fazer 4 configurações adicionais
        //1 - Registrar as classes
        //2 - Definir os tipos de saída (chave, valor) do Map e do Reduce
        //3 - Cadastrar os arquivos de entrada e saída
        //4 - Lançar o exit

        //1 - Registrar as classes
        j.setJarByClass(WordCount.class);
        j.setReducerClass(ReduceForWordCount.class);
        j.setMapperClass(MapForWordCount.class);

        //2 - Definir os tipos de saída (chave, valor) do Map e do Reduce
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(IntWritable.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(IntWritable.class);

        //3 - Cadastrar os arquivos de entrada e saída
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        //4 - Lançar o exit
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    public static class MapForWordCount extends Mapper<LongWritable, Text, Text, IntWritable> {

        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            String linha = value.toString();
            String[] words = linha.split(" ");
            for (String w : words) {
                //formatando caracteres especiais
                w = w.replaceAll("[^a-zA-Z0-9]", "");
                if (!w.isEmpty()) { // Evitar palavras vazias
                    Text word = new Text(w);
                    IntWritable freq = new IntWritable(1);
                    //Salvando somente a palavra "Do"
                    if(word.toString().equals("Do")){
                        con.write(word, freq);
                    }
                }
            }
        }
    }

    public static class ReduceForWordCount extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context con)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable v : values) {
                sum += v.get();
            }
            con.write(key, new IntWritable(sum));
        }
    }

}
