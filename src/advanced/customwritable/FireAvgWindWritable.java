package advanced.customwritable;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FireAvgWindWritable implements Writable{

    private float wind;
    private int freq;

    //Passo a passo do que foi feito:
    //Criar o construtor vazio
    //Criar o construtor com os parametros
    //Criar os getters e setters
    //Implementar os metodos write e readFields
    //No metodo write, escrever os atributos usando dataOutput
    //No metodo readFields, ler os atributos usando dataInput
    //Usar Float.parseFloat e Integer.parseInt para converter os valores lidos
    //Usar dataOutput.writeUTF e dataInput.readUTF para escrever e ler os valores

    public FireAvgWindWritable() {
        this.wind = 0;
        this.freq = 0;
    }

    public FireAvgWindWritable(float wind, int freq) {
        this.wind = wind;
        this.freq = freq;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(String.valueOf(wind));
        dataOutput.writeUTF(String.valueOf(freq));
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        wind = Float.parseFloat(dataInput.readUTF());
        freq = Integer.parseInt(dataInput.readUTF());
    }

    public float getWind() {
        return wind;
    }

    public void setWind(float wind) {
        this.wind = wind;
    }

    public int getFreq() {
        return freq;
    }

    public void setFreq(int freq) {
        this.freq = freq;
    }
}
