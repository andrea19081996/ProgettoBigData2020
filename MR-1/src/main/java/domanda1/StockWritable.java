package domanda1;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class StockWritable implements WritableComparable<StockWritable> {
    DoubleWritable prezzo_chiusura;
    LongWritable volume;
    Text data;

    public StockWritable(DoubleWritable prezzo_chiusura, LongWritable volume, Text data) {
        this.prezzo_chiusura = prezzo_chiusura;
        this.volume = volume;
        this.data = data;
    }

    public StockWritable() {

    }

    public void setPrezzo_chiusura(DoubleWritable prezzo_chiusura) {
        this.prezzo_chiusura = prezzo_chiusura;
    }

    public void setVolume(LongWritable volume) {
        this.volume = volume;
    }

    public void setData(Text data) {
        this.data = data;
    }

    public DoubleWritable getPrezzo_chiusura() {
        return prezzo_chiusura;
    }

    public LongWritable getVolume() {
        return volume;
    }

    public Text getData() {
        return data;
    }


    public int compareTo(StockWritable o) {
        int result = this.data.compareTo(o.getData());
        if (result != 0)  return result;
            else {
                result = this.prezzo_chiusura.compareTo(o.getPrezzo_chiusura());
                if (result != 0) return result;
                else  {
                    result = this.volume.compareTo(o.getVolume());
                    return result;
                }
            }
    }

    public void write(DataOutput out) throws IOException {
        out.writeDouble(this.prezzo_chiusura.get());
        out.writeLong(this.volume.get());
        out.writeUTF(this.data.toString());
    }

    public void readFields(DataInput in) throws IOException {
        this.prezzo_chiusura= new DoubleWritable(in.readDouble());
        this.volume= new LongWritable(in.readLong());
        this.data= new Text(in.readUTF());
    }

    @Override
    public String toString() {
        return "domanda1.StockWritable{" +
                "prezzo_chiusura=" + prezzo_chiusura +
                ", volume=" + volume +
                ", data=" + data +
                '}';
    }
}
