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
        Path output = new Path("output/saida_com_combiner");

        // criacao do job e seu nome
        Job j = new Job(c, "mediawind");

        //Precisamos fazer 4 configurações adicionais
        //1 - Registrar as classes
        //2 - Definir os tipos de saída (chave, valor) do Map e do Reduce
        //3 - Cadastrar os arquivos de entrada e saída
        //4 - Lançar o exit

        //1 - Registrar as classes
        j.setJarByClass(AverageWind.class);
        j.setMapperClass(MapForAverage.class);
        j.setReducerClass(ReduceForAverage.class);
        //O combiner vai ser usado para fazer uma pré-agrupamento dos dados
        //Ele é opcional, pode comentar essa linha para ver a diferença
        //O resultado final não muda, mas o tempo de execução pode melhorar
        //Deixei imagens dentro da pasta 'imagens' para mostrar a diferença
        j.setCombinerClass(CombineForAverage.class);

        //2 - Definir os tipos de saída (chave, valor) do Map e do Reduce
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(FireAvgWindWritable.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(FloatWritable.class);

        //3 - Definir os arquivos de entrada e saída
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        //4 - Lançar o exit
        System.exit(j.waitForCompletion(true)?0:1);
    }

    //A função do map é emitir a chave 'mês' e o valor 'vento'
    public static class MapForAverage extends Mapper<LongWritable, Text, Text, FireAvgWindWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            //entrada 7,5,mar,fri,86.2,26.2,94.3,5.1,8.2,51,6.7,0,0
            //Pegando cada linha do arquivo
            String linha = value.toString();

            //Separando os valores por virgula
            String[] valores = linha.split(",");

            //Pegando o mes(coluna 3) e a coluna 10 q é o vento
            String mes = valores[2];
            float vento = Float.parseFloat(valores[10]);

            //Emitindo a chave 'mes' e o valor 'vento'
            //<mes, <vento, frequencia>>
            con.write(new Text(mes), new FireAvgWindWritable(vento, 1));
        }
    }

    //A função do combiner é fazer uma pré-agrupamento dos dados
    //Dessa forma o reduce terá menos dados para processar
    //Ele vai somar somente os ventos e as frequencias de cada mês ao invés
    // de enviar todos os ventos e frequencias para o reduce
    public static class CombineForAverage extends Reducer<Text, FireAvgWindWritable, Text, FireAvgWindWritable> {
        public void reduce(Text key, Iterable<FireAvgWindWritable> values, Context con)
                throws IOException, InterruptedException {
            //<key, [(20, 1), (30, 1), .....>
            int somaF = 0;
            float somaW = 0;

            //Somar todos os ventos e todas as frequencias de um mesmo mês (key)
            for(FireAvgWindWritable v : values){
                somaW += v.getWind();
                somaF += v.getFreq();
            }

            //Emitir a soma dos ventos e a soma das frequencias
            //<mes, <somaVento, somaFrequencia>>
            con.write(key, new FireAvgWindWritable(somaW, somaF));
        }
    }

    //A função do reduce é agrupar os valores por chave e calcular a média
    public static class ReduceForAverage extends Reducer<Text, FireAvgWindWritable, Text, FloatWritable> {
        public void reduce(Text key, Iterable<FireAvgWindWritable> values, Context con)
                throws IOException, InterruptedException {
            //<key, [(20, 1), (30, 1), .....>
            //<mes, <vento, frequencia>>
            //Nosso objetivo é fazer o cálculo da média do vento por mês
            int somaN = 0;
            float somaW = 0;

            //Somar todos os ventos e todas as frequencias
            for(FireAvgWindWritable v : values){
                somaW += v.getWind();
                somaN += v.getFreq();
            }

            //Calcular a média da velocidade do vento
            float media = somaW / somaN;

            //Escrever o mês e a média do vento
            //<mes, mediaVento>
            con.write(key, new FloatWritable(media));
        }
    }

}
