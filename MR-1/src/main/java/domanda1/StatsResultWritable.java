package domanda1;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class StatsResultWritable implements WritableComparable<StatsResultWritable> {

    DoubleWritable var_quotazione;
    DoubleWritable prezzo_minimo;
    DoubleWritable prezzo_massimo;
    DoubleWritable volume_medio;

    public StatsResultWritable(DoubleWritable var_quotazione, DoubleWritable prezzo_minimo, DoubleWritable prezzo_massimo, DoubleWritable volume_medio) {
        this.var_quotazione = var_quotazione;
        this.prezzo_minimo = prezzo_minimo;
        this.prezzo_massimo = prezzo_massimo;
        this.volume_medio = volume_medio;
    }

    public StatsResultWritable(){

    }

    public DoubleWritable getVar_quotazione() {
        return var_quotazione;
    }

    public void setVar_quotazione(DoubleWritable var_quotazione) {
        this.var_quotazione = var_quotazione;
    }

    public DoubleWritable getPrezzo_minimo() {
        return prezzo_minimo;
    }

    public void setPrezzo_minimo(DoubleWritable prezzo_minimo) {
        this.prezzo_minimo = prezzo_minimo;
    }

    public DoubleWritable getPrezzo_massimo() {
        return prezzo_massimo;
    }

    public void setPrezzo_massimo(DoubleWritable prezzo_massimo) {
        this.prezzo_massimo = prezzo_massimo;
    }

    public DoubleWritable getVolume_medio() {
        return volume_medio;
    }

    public void setVolume_medio(DoubleWritable volume_medio) {
        this.volume_medio = volume_medio;
    }

    public int compareTo(StatsResultWritable o) {
        int result = this.var_quotazione.compareTo(o.getVar_quotazione());
        if (result != 0)  return result;
        else {
            result = this.prezzo_minimo.compareTo(o.getPrezzo_minimo());
            if (result != 0) return result;
            else  {
                result = this.volume_medio.compareTo(o.getVolume_medio());
                if (result!=0) return result;
                else {
                    return this.prezzo_massimo.compareTo(o.getPrezzo_massimo());

                }
            }
        }
    }

    @Override
    public String toString() {
        return  "var_quotazione=" + var_quotazione +
                ", prezzo_minimo=" + prezzo_minimo +
                ", prezzo_massimo=" + prezzo_massimo +
                ", volume_medio=" + volume_medio;
    }

    public void write(DataOutput out) throws IOException {
        out.writeDouble(this.var_quotazione.get());
        out.writeDouble(this.prezzo_minimo.get());
        out.writeDouble(this.prezzo_massimo.get());
        out.writeDouble(this.volume_medio.get());
    }

    public void readFields(DataInput in) throws IOException {
        this.var_quotazione= new DoubleWritable(in.readDouble());
        this.prezzo_minimo= new DoubleWritable(in.readDouble());
        this.prezzo_massimo= new DoubleWritable(in.readDouble());
        this.volume_medio= new DoubleWritable(in.readDouble());
    }

}
