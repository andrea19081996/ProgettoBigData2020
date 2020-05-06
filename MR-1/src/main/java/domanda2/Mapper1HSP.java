package domanda2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.StringTokenizer;

/**
 * Classe mapper dle job 1: esegue il join tra historical_stock_prices e historical_stocks.
 * In particolare questo mapper ha il compito di esaminare il file historical_stock_prices.csv
 */
public class Mapper1HSP extends Mapper<LongWritable, Text, Text, Text> {
    Logger logger = LoggerFactory.getLogger(Mapper1HSP.class);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //logger.info("Analizzo riga: " + value.toString());
        StringTokenizer st = new StringTokenizer(value.toString(), ",");
        String ticker;
        Double chiusura;

        ticker = getToken(st);
        if(ticker==null) return;
        if(getToken(st)==null) return;  // Salto il prezzo di apertura
        try{
            chiusura = Double.valueOf(getToken(st));
        }catch(Exception e) {
            logger.error("Impossibile parsare il prezzo di chiusura, riga scartata.");
            return;
        }
        // Salto adj_close, minimo, massimo e volume
        if(getToken(st)==null) return;
        if(getToken(st)==null) return;
        if(getToken(st)==null) return;
        if(getToken(st)==null) return;

        String data;
        try {
            data = getToken(st);
            int anno = Integer.parseInt(data.substring(0, 4));
            if(anno<2016 || anno>2018)
                return; // Scarto il dato
        }catch(Exception e ) {
            logger.error("Impossibile parsare la data, riga scartata.");
            return;
        }
        //logger.info("Emetto elemento: " + ticker + " hsp," + data + "," + chiusura);
        context.write(new Text(ticker), new Text("hsp," + data + "," + chiusura));

    }

    private String getToken(StringTokenizer st) {
        if(st.hasMoreTokens())
            return st.nextToken();
        else{
            logger.error("Impossibile parsare, token mancante.");
            return null;
        }
    }
}
