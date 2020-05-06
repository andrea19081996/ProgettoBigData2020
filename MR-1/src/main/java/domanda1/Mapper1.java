package domanda1;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.StringTokenizer;

public class Mapper1 extends
        Mapper<LongWritable, Text, Text, StockWritable> {

    private Logger logger = LoggerFactory.getLogger(Mapper1.class);
    @Override
    protected void map(LongWritable key, Text value,
                       Context ctx) throws IOException, InterruptedException {
        StringTokenizer tokenizer = new StringTokenizer(value.toString(), ",");
        if(tokenizer.countTokens()!=8) {
            logger.error("La riga non contiene 8 campi. Riga scartata.");
            return;
        }

        StockWritable stock = new StockWritable();
        String ticker;

        if(tokenizer.hasMoreTokens()) {
            ticker = tokenizer.nextToken();
        }else
        {
            logger.error("Impossibile leggere il Ticker, token non presente, riga scartata.");
            return;
        }

        tokenizer.nextToken();  // Scarto il prezzo di apertura
        if(tokenizer.hasMoreTokens()) {
            try {
                stock.setPrezzo_chiusura(new DoubleWritable(Double.parseDouble(tokenizer.nextToken())));
            } catch (NumberFormatException e) {
                //e.printStackTrace();
                logger.error("Errore nel parsing del prezzo di chiusura: " + e.getMessage() + " - Riga scartata.");
                return;
            }
        }else
        {
            logger.error("Impossibile leggere il prezzo di chiusura, token non presente, riga scartata.");
            return;
        }
        // Salto il prezzo massimo, minimo e il prezzo di chiusura aggiustato
        tokenizer.nextToken();
        tokenizer.nextToken();
        tokenizer.nextToken();

        if(tokenizer.hasMoreTokens()) {
            try {
                stock.setVolume(new LongWritable(Long.parseLong(tokenizer.nextToken())));
            } catch (NumberFormatException e) {
                //e.printStackTrace();
                logger.error("Errore nel parsing del volume: " + e.getMessage() + " - Riga scartata.");
                return;
            }
        }else
        {
            logger.error("Impossibile leggere il volume, token non presente, riga scartata.");
            return;
        }

        if(tokenizer.hasMoreTokens()) stock.setData(new Text(tokenizer.nextToken()));
        else
        {
            logger.error("Impossibile leggere la data, token non presente, riga scartata.");
            return;
        }

        int anno;
        try {
            // Filtro sull'anno
            anno = Integer.parseInt(stock.getData().toString().substring(0, 4));
            if (anno < 2008 || anno > 2018)
                return;
        }catch(Exception e )
        {
            logger.error("Errore nel parsing dell'anno: " + e.getMessage() + " - Riga scartata.");
            return;
        }
        Text valueKey=new Text(ticker);
        ctx.write(valueKey, stock);
    }

}