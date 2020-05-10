package domanda3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Classe mapper dle job 1: esegue il join tra historical_stock_prices e historical_stocks.
 * In particolare questo mapper ha il compito di esaminare il file historical_stock.csv
 */
public class Mapper1HS extends Mapper<LongWritable, Text, Text, Text> {
    Logger logger = LoggerFactory.getLogger(Mapper1HS.class);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"); // Divido la stringa in 3 parti: Ticker, exchange e resto della stringa

        if(parts.length==5) {
            String ticker = parts[0];
            String nome = parts[2];
            //logger.info("Emetto elemento: " + ticker + " " + nome);
            context.write(new Text(ticker), new Text("hs," + nome));
        }


    }
}
