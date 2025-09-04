package advanced.customwritable;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;



public class WindTempWritable implements Writable{

    private float wind;
    private float temp;

    public WindTempWritable() {
        this.wind = 0;
        this.temp = 0;
    }

    public WindTempWritable(float wind, float temp) {
        this.wind = wind;
        this.temp = temp;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(String.valueOf(wind));
        out.writeUTF(String.valueOf(temp));
    }


    @Override
    public void readFields(DataInput in) throws IOException {
        wind = Float.parseFloat(in.readUTF());
        temp = Float.parseFloat(in.readUTF());
    }

    public float getWind() {
        return wind;
    }

    public void setWind(float wind) {
        this.wind = wind;
    }

    public float getTemp() {
        return temp;
    }

    public void setTemp(float temp) {
        this.temp = temp;
    }
}
