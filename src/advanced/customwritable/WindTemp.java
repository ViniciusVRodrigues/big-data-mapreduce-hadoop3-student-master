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





public class WindTemp{

    public static void main(String args[]) throws IOException,
            ClassNotFoundException,
            InterruptedException {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path("in/forestfireinput.csv");

        // arquivo de saida
        Path output = new Path("output/wind_temp_max_com_combiner.txt");

        // criacao do job e seu nome
        Job j = new Job(c, "WindTemp");

        //O objetivo dessa tarefa é pegar o vento e a temperatura máxima de cada mês

        //1. registrar classes
        //2. definir tipos de saídas
        //3. cadastrar os arquivos de entrada e saída
        //4. lancar o job

        //1.registro das classes
        j.setJarByClass(WindTemp.class);
        j.setMapperClass(MapForWindTemp.class);
        j.setReducerClass(ReduceForWindTemp.class);
        //O combiner vai ser usado para fazer uma pré-agrupamento dos dados
        //Nessa tarefa ele vai fazer o mesmo trabalho do reduce
        //Ele é opcional, pode comentar essa linha para ver a diferença
        j.setCombinerClass(CombineForWindTemp.class);

        //2. definir tipos de saídas
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(WindTempWritable.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(Text.class);

        //3. definir arquivos de entrada e saída
        FileInputFormat.addInputPath(j,input);
        FileOutputFormat.setOutputPath(j,output);

        //4. lancar o job
        System.exit(j.waitForCompletion(true)?0:1);
    }


    //A função do map nessa tarefa vai ser emitir a chave 'mês' e o valor 'vento, temperatura máxima'
    //Portanto o WindTempWritable deve ser ajustado para armazenar esses dois valores
    public static class MapForWindTemp extends Mapper<LongWritable, Text, Text, WindTempWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            //entrada 7,5,mar,fri,86.2,26.2,94.3,5.1,8.2,51,6.7,0,0
            //Mês (2), Temp (8), Vento(10)

            String linha = value.toString();

            String[] valores = linha.split(",");

            //Pegar os valores de mês, temperatura e vento
            String month = valores[2];
            float temp = Float.parseFloat(valores[8]);
            float wind = Float.parseFloat(valores[10]);

            //Emitir a chave 'mês' e o valor 'vento, temperatura'
            con.write(new Text(month), new WindTempWritable(wind, temp));
        }
    }

    //O combiner vai fazer um pré-agrupamento dos dados
    //Ele vai receber a chave 'mês' e uma lista de valores 'vento, temperatura'
    //E vai retornar o vento e a temperatura máxima daquele mês
    public static class CombineForWindTemp extends Reducer<Text, WindTempWritable, Text, WindTempWritable>{
        public void reduce(Text key, Iterable<WindTempWritable> values, Context con)
                throws IOException, InterruptedException {

            //Inicializar as variáveis de máximo
            //Float.MIN_VALUE é o menor valor que um float pode ter,
            //vamos usar ele para iniciar a comparação
            float maxTemp = Float.MIN_VALUE;
            float maxWind = Float.MIN_VALUE;

            //Percorrer a lista de valores e encontrar o vento e a temperatura máxima
            for(WindTempWritable v : values){
                if(v.getTemp() > maxTemp){
                    maxTemp = v.getTemp();
                }
                if(v.getWind() > maxWind) {
                    maxWind = v.getWind();
                }
            }

            //Emitir o vento e a temperatura máxima daquele mês
            con.write(key, new WindTempWritable(maxWind, maxTemp));
        }
    }

    //O reduce vai receber a chave 'mês' e uma lista de valores 'vento, temperatura'
    //E vai retornar o vento e a temperatura máxima daquele mês
    public static class ReduceForWindTemp extends Reducer<Text, WindTempWritable, Text, Text> {
        public void reduce(Text key, Iterable<WindTempWritable> values, Context con)
                throws IOException, InterruptedException {
            //Inicializar as variáveis de máximo
            float maxTemp = Float.MIN_VALUE;
            float maxWind = Float.MIN_VALUE;

            //Percorrer a lista de valores e encontrar o vento e a temperatura máxima
            for(WindTempWritable v : values){
                if(v.getTemp() > maxTemp){
                    maxTemp = v.getTemp();
                }
                if(v.getWind() > maxWind) {
                    maxWind = v.getWind();
                }
            }

            //Emitir o mês, o vento máximo e a temperatura máxima
            con.write(key, new Text("Max Wind: " + maxWind + ", Max Temp: " + maxTemp));
        }
    }

}
