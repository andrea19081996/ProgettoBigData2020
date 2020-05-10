import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class JoinResult implements WritableComparable<JoinResult> {

    DoubleWritable volume_medio;
    DoubleWritable variazione_annuale;
    DoubleWritable quotazione_giornaliera_media;

    @Override
    public String toString() {
        return
                "{volume_medio," + volume_medio +
                ",variazione_annuale," + variazione_annuale +
                ",quotazione_giornaliera_media," + quotazione_giornaliera_media +
                '}';
    }


    public DoubleWritable getVolume_medio() {
        return volume_medio;
    }

    public DoubleWritable getVariazione_annuale() {
        return variazione_annuale;
    }

    public DoubleWritable getQuotazione_giornaliera_media() {
        return quotazione_giornaliera_media;
    }


    public void setVolume_medio(DoubleWritable volume_medio) {
        this.volume_medio = volume_medio;
    }

    public void setVariazione_annuale(DoubleWritable variazione_annuale) {
        this.variazione_annuale = variazione_annuale;
    }

    public void setQuotazione_giornaliera_media(DoubleWritable quotazione_giornaliera_media) {
        this.quotazione_giornaliera_media = quotazione_giornaliera_media;
    }

    public JoinResult() {

    }

    public int compareTo(JoinResult o) {
        int result = this.volume_medio.compareTo(o.getVolume_medio());
        if (result != 0)  return result;
        else {
            result = this.variazione_annuale.compareTo(o.getVariazione_annuale());
            if (result != 0) return result;
            else  {
                return this.quotazione_giornaliera_media.compareTo(o.getQuotazione_giornaliera_media());
            }
        }
    }

    public void write(DataOutput out) throws IOException {

        out.writeDouble(this.quotazione_giornaliera_media.get());
        out.writeDouble(this.volume_medio.get());
        out.writeDouble(this.variazione_annuale.get());
    }

    public void readFields(DataInput in) throws IOException {

        this.quotazione_giornaliera_media= new DoubleWritable(in.readDouble());
        this.volume_medio= new DoubleWritable(in.readDouble());
        this.variazione_annuale = new DoubleWritable(in.readDouble());
    }
}