package domanda2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Classe mapper dle job 1: esegue il join tra historical_stock_prices e historical_stocks.
 * In particolare questo mapper ha il compito di esaminare il file historical_stock.csv
 */
public class Mapper1HS extends Mapper<LongWritable, Text, Text, Text> {
    Logger logger = LoggerFactory.getLogger(Mapper1HS.class);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split(",",3); // Divido la stringa in 3 parti: Ticker, exchange e resto della stringa
        String ticker = parts[0];
        String nome;
        try {
            int endName = parts[2].indexOf("\"", 1);
            nome = parts[2].substring(1, endName);
            //logger.info("Emetto elemento: " + ticker + " " + nome);
            context.write(new Text(ticker), new Text("hs," + nome));
        }catch (Exception e) {
            logger.error("Impossibile parsare il nome, provo con un altro metodo.");
            try {
                nome = parts[2].split(",")[0];
                //logger.info("Emetto elemento: " + ticker + " " + nome);
                context.write(new Text(ticker), new Text("hs," + nome));
            }catch(Exception e2) {
                logger.error("Impossibile parsare il nome, riga scartata");
                return;
            }

        }

    }
}
