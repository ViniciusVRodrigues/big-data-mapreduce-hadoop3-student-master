package advanced.customwritable;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.checkerframework.checker.units.qual.Temperature;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class FireAvgTempWritable implements Writable{

    private Float temperature;
    private Integer frequency;

    public FireAvgTempWritable() {
    }

    public FireAvgTempWritable(Float temperature, Integer frequency) {
        this.temperature = temperature;
        this.frequency = frequency;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(String.valueOf(temperature));
        dataOutput.writeUTF(String.valueOf(frequency));
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.temperature = Float.parseFloat(dataInput.readUTF());
        this.frequency = Integer.parseInt(dataInput.readUTF());
    }

    public Float getTemperature() {
        return temperature;
    }

    public Integer getFrequency() {
        return frequency;
    }

    @Override
    public String toString() {
        return "FireAvgTempWritable{" +
                "temperature=" + temperature +
                ", frequency=" + frequency +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FireAvgTempWritable that = (FireAvgTempWritable) o;
        return Objects.equals(temperature, that.temperature) && Objects.equals(frequency, that.frequency);
    }

    @Override
    public int hashCode() {
        return Objects.hash(temperature, frequency);
    }
}
