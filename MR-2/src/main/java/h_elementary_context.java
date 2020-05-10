import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class h_elementary_context implements WritableComparable<h_elementary_context> {

    DoubleWritable prezzo_chisura;
    DoubleWritable volume;
    DoubleWritable variazione_annuale;
    DoubleWritable contatore;




    @Override
    public String toString() {
        return
                "prezzo_chisura," + prezzo_chisura +
                ",volume," + volume +
                ",variazione_annuale," + variazione_annuale +
                ",contatore," + contatore;
    }

    public DoubleWritable getPrezzo_chisura() {
        return prezzo_chisura;
    }

    public void setPrezzo_chisura(DoubleWritable prezzo_chisura) {
        this.prezzo_chisura = prezzo_chisura;
    }

    public DoubleWritable getVolume() {
        return volume;
    }

    public void setVolume(DoubleWritable volume) {
        this.volume = volume;
    }

    public DoubleWritable getContatore() {
        return contatore;
    }

    public void setContatore(DoubleWritable contatore) {
        this.contatore = contatore;
    }

    public DoubleWritable getVariazione_annuale() {
        return variazione_annuale;
    }

    public void setVariazione_annuale(DoubleWritable variazione_annuale) {
        this.variazione_annuale = variazione_annuale;
    }

    public h_elementary_context(){

    }

    public int compareTo(h_elementary_context o) {
        int result = this.prezzo_chisura.compareTo(o.getPrezzo_chisura());
        if (result != 0)  return result;
        else {
            result = this.variazione_annuale.compareTo(o.getVariazione_annuale());
            if (result != 0) return result;
            else  {
                result= this.contatore.compareTo(o.getContatore());
                if(result!=0) return result;
                else {
                    return this.volume.compareTo(o.getVolume());
                }
            }
        }
    }

    public void write(DataOutput out) throws IOException {
        out.writeDouble(this.prezzo_chisura.get());
        out.writeDouble(this.volume.get());
        out.writeDouble(this.variazione_annuale.get());
        out.writeDouble(this.contatore.get());
    }

    public void readFields(DataInput in) throws IOException {
        this.prezzo_chisura = new DoubleWritable(in.readDouble());
        this.volume= new DoubleWritable(in.readDouble());
        this.variazione_annuale= new DoubleWritable(in.readDouble());
        this.contatore= new DoubleWritable(in.readDouble());



    }
}
