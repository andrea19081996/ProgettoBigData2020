package domanda1;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Reducer1 extends
        Reducer<Text, StockWritable,Text, TempOutputStatsResultWritable> {

    //classe ausiliaria
    private class PrezzoChiusuraConData{

        private Double prezzo;
        private Date data;

        public PrezzoChiusuraConData() {

        }

        public Double getPrezzo() {
            return prezzo;
        }

        public void setPrezzo(Double prezzo) {
            this.prezzo = prezzo;
        }

        public Date getData() {
            return data;
        }

        public void setData(Date data) {
            this.data = data;
        }

        public PrezzoChiusuraConData(Double prezzo, Date data) {
            this.prezzo = prezzo;
            this.data = data;
        }
    }


    @Override
    protected void reduce(Text key, Iterable<StockWritable> values, Context context) throws IOException, InterruptedException {
        // Prezzo minimo e massimo di chiusura
        Double prezzoMin= (double) 0;
        Double prezzoMax= (double) 0;

        PrezzoChiusuraConData prChiusuraIniziale= new PrezzoChiusuraConData();
        PrezzoChiusuraConData prChiusuraFinale = new PrezzoChiusuraConData();

        Long count = 0L;
        Double sum_volume= 0.0;

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

        if(values.iterator().hasNext()) {
            // Inizializzo il prezzo minimo e massimo di chiusura, e i prezzi di chiusura iniziale e finale del periodo
            StockWritable s = values.iterator().next();
            prezzoMax = s.getPrezzo_chiusura().get();
            prezzoMin = s.getPrezzo_chiusura().get();
            try {
                prChiusuraIniziale.setData(sdf.parse(s.getData().toString()));
                prChiusuraIniziale.setPrezzo(s.getPrezzo_chiusura().get());
            } catch (ParseException e) {
                e.printStackTrace();
            }
            try {
                prChiusuraFinale.setData(sdf.parse(s.getData().toString()));
                prChiusuraFinale.setPrezzo(s.getPrezzo_chiusura().get());
            } catch (ParseException e) {
                e.printStackTrace();
            }

            // Aggiorno i dati sul volume
            sum_volume+= s.getVolume().get();
            count++;
        }

        Date auxDate = null;

        for(StockWritable stock : values) {

            sum_volume += stock.getVolume().get();
            count++;

            // Controllo i prezzi di chiusura massimo e minimo
            if(stock.getPrezzo_chiusura().get() < prezzoMin)
                prezzoMin = stock.getPrezzo_chiusura().get();
            if(stock.getPrezzo_chiusura().get() > prezzoMax)
                prezzoMax = stock.getPrezzo_chiusura().get();


            try {
                // Controllo i prezzi di chiusura iniziale e finale del periodo
                auxDate = sdf.parse(stock.getData().toString());
                if(auxDate.compareTo(prChiusuraFinale.getData())>0) {
                    prChiusuraFinale.setData(auxDate);
                    prChiusuraFinale.setPrezzo(stock.getPrezzo_chiusura().get());
                }
                if(auxDate.compareTo(prChiusuraIniziale.getData())<0) {
                    prChiusuraIniziale.setData(auxDate);
                    prChiusuraIniziale.setPrezzo(stock.getPrezzo_chiusura().get());
                }
            } catch (ParseException e) {
                e.printStackTrace();
            }


        }

        sum_volume= sum_volume/count;

        double var_quotazione= ((prChiusuraFinale.getPrezzo()- prChiusuraIniziale.getPrezzo())/prChiusuraIniziale.getPrezzo())*100;
        var_quotazione = (double) Math.round(var_quotazione * 100) / 100;

        TempOutputStatsResultWritable result = new TempOutputStatsResultWritable();
        result.setVar_quotazione(new DoubleWritable(var_quotazione));
        result.setPrezzo_minimo(new DoubleWritable(prezzoMin));
        result.setPrezzo_massimo(new DoubleWritable(prezzoMax));
        result.setVolume_medio(new DoubleWritable(sum_volume));

        context.write(key, result);
    }
}