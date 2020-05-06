package domanda1;


import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class Reducer2 extends
        Reducer<Text, StatsResultWritable,Text, StatsResultWritable> {

    private List<Pair<String, StockResult>> resultList;

    private class Pair<F,S> {
        F key;
        S value;

        public Pair(F key, S value) {
            this.key = key;
            this.value = value;
        }
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        this.resultList = new LinkedList<>();
    }

    @Override
    protected void reduce(Text key, Iterable<StatsResultWritable> values, Context context) throws IOException, InterruptedException {
        StatsResultWritable o = values.iterator().next();
        StockResult stock = new StockResult();
        stock.setPrezzo_massimo(o.getPrezzo_massimo().get());
        stock.setPrezzo_minimo(o.getPrezzo_minimo().get());
        stock.setVar_quotazione(o.getVar_quotazione().get());
        stock.setVolume_medio(o.getVolume_medio().get());
        this.resultList.add(new Pair<String, StockResult>(key.toString(), stock));
        //context.write(key,values.iterator().next());
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        this.resultList.sort((p1, p2) -> -1*(p1.value.getVar_quotazione().compareTo(p2.value.getVar_quotazione())) );

        for(Pair<String,StockResult> p : this.resultList) {
            StatsResultWritable o = new StatsResultWritable();
            o.setVar_quotazione(new DoubleWritable(p.value.getVar_quotazione()));
            o.setPrezzo_minimo(new DoubleWritable(p.value.getPrezzo_minimo()));
            o.setPrezzo_massimo(new DoubleWritable(p.value.getPrezzo_massimo()));
            o.setVolume_medio(new DoubleWritable(p.value.getVolume_medio()));
            context.write(new Text(p.key), o);
        }
        super.cleanup(context);
    }

}