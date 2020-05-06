package domanda1;


import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Serializable;
import scala.Tuple2;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Objects;
import java.util.StringTokenizer;

public class Domanda1 {

    //Classi di supporto
    private static class Stock implements Serializable {
        public String ticker;
        public Double close;
        public Double volume;
        public Date date;

    }

    private static class StockResultTemp implements Serializable {
        public String ticker;
        public Double prezzoMinimo;
        public Double prezzoMassimo;
        public Date dataIniziale;
        public Double prezzoChiusuraIniziale;
        public Date dataFinale;
        public Double prezzoChiusuraFinale;
        public Double volumeSum;
        public Long volumeCount;
    }

    private static class Result implements Serializable {
        public String key;
        public Double prezzoMinimo;
        public Double prezzoMassimo;
        public Double variazione;
        public Double volumeMedio;

        @Override
        public String toString() {
            return key + " Variazione: " + variazione.toString() + " Prezzo massimo: "  + prezzoMassimo.toString() + " Prezzo minimo: " + prezzoMinimo.toString()
                    + " Volume medio: " + volumeMedio.toString();

        }
    }



    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(Domanda1.class);
        if(args.length<2) {
            logger.error("Devi passare un parametro: percoros del file di input e percorso del file di output.");
            System.exit(-1);
        }
        SparkSession spark = SparkSession.builder().appName("Domanda 1").getOrCreate();
        JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

        // Trasformo le righe csv in oggetti e filtro quelli di interesse (2008<x<2018)
        JavaRDD<Stock> stocksData = lines.map((Function<String, Stock>) line -> {

            // Durante la conversione controllo i campi. Ci sono dati spuri, le righe che non è possibile parsare le scarto.
            StringTokenizer st = new StringTokenizer(line,",");
            Stock stock = new Stock();
            if(st.hasMoreTokens())
                stock.ticker=st.nextToken();
            else {
                logger.error("Impossibile parsare il ticker. Token non presente.");
                return null;
            }
            st.nextToken(); // Scarto open
            if(st.hasMoreTokens())
                try {
                    stock.close = Double.valueOf(st.nextToken());
                }catch (NumberFormatException e) {
                    logger.error("Impossibile parsare il prezzo di chiusura. Token mal formattato.");
                    return null;
                }
            else {
                logger.error("Impossibile parsare il prezzo di chiusura. Token non presente.");
                return null;
            }
            st.nextToken(); // Scarto adj_close, high e low
            st.nextToken();
            st.nextToken();

            if(st.hasMoreTokens())
                try {
                    stock.volume = Double.valueOf(st.nextToken());
                }catch (NumberFormatException e) {
                    logger.error("Impossibile parsare il volume. Token mal formattato.");
                    return null;
                }
            else {
                logger.error("Impossibile parsare il volume. Token non presente.");
                return null;
            }
            if(st.hasMoreTokens())
                try {
                    stock.date = sdf.parse(st.nextToken());
                    if(stock.date.after(sdf.parse("2007-12-31")) && stock.date.before(sdf.parse("2019-01-01")) )
                        return stock;
                }catch (ParseException e) {
                    logger.error("Impossibile parsare la data. Data mal formattata.");
                    return null;
                }
            else {
                logger.error("Impossibile parsare la data. Token non presente.");
                return null;
            }
            return null;
        });
        stocksData = stocksData.filter(Objects::nonNull);   // Tolgo oggetti con dati spuri

        // Eseguo la map utilizzando una classe di appoggio che mi permette di elaborare facilmente il risultato nella reduce
        JavaPairRDD<String, StockResultTemp> mapped = stocksData.mapToPair((PairFunction<Stock, String, StockResultTemp>) stock -> {
            StockResultTemp stockResultTemp = new StockResultTemp();
            stockResultTemp.prezzoMassimo = stock.close;
            stockResultTemp.prezzoMinimo = stock.close;
            stockResultTemp.dataFinale = stock.date;
            stockResultTemp.prezzoChiusuraFinale = stock.close;
            stockResultTemp.dataIniziale = stock.date;
            stockResultTemp.prezzoChiusuraIniziale = stock.close;
            stockResultTemp.ticker=stock.ticker;
            stockResultTemp.volumeCount=1L;
            stockResultTemp.volumeSum = stock.volume;
            return new Tuple2<String, StockResultTemp>(stock.ticker, stockResultTemp);
        });

        JavaPairRDD<String, StockResultTemp> reduced = mapped.reduceByKey((Function2<StockResultTemp, StockResultTemp, StockResultTemp>) (s1, s2) -> {
            StockResultTemp result = new StockResultTemp();
            result.volumeSum = s1.volumeSum + s2.volumeSum;
            result.volumeCount = s1.volumeCount + s2.volumeCount;
            result.ticker = s1.ticker;

            if (s1.prezzoMassimo.compareTo(s2.prezzoMassimo) > 0) result.prezzoMassimo = s1.prezzoMassimo;
            else result.prezzoMassimo = s2.prezzoMassimo;

            if (s1.prezzoMinimo.compareTo(s2.prezzoMinimo) < 0) result.prezzoMinimo = s1.prezzoMinimo;
            else result.prezzoMinimo = s2.prezzoMinimo;

            if (s1.dataIniziale.compareTo(s2.dataIniziale) < 0) {
                result.dataIniziale = s1.dataIniziale;
                result.prezzoChiusuraIniziale = s1.prezzoChiusuraIniziale;
            } else {
                result.dataIniziale = s2.dataIniziale;
                result.prezzoChiusuraIniziale = s2.prezzoChiusuraIniziale;
            }
            if (s1.dataFinale.compareTo(s2.dataFinale) > 0) {
                result.dataFinale = s1.dataFinale;
                result.prezzoChiusuraFinale = s1.prezzoChiusuraFinale;
            } else {
                result.dataFinale = s2.dataFinale;
                result.prezzoChiusuraFinale = s2.prezzoChiusuraFinale;
            }
            return result;
        });

        // Seleziono e calcolo i campi che mi interessano
        JavaRDD<Result> result = reduced.map((Function<Tuple2<String, StockResultTemp>, Result>) tupla -> {
            Result result1 = new Result();
            result1.key = tupla._1;
            result1.prezzoMassimo=tupla._2.prezzoMassimo;
            result1.prezzoMinimo=tupla._2.prezzoMinimo;
            result1.volumeMedio = tupla._2.volumeSum/tupla._2.volumeCount;
            result1.variazione = (double) Math.round((((tupla._2.prezzoChiusuraFinale - tupla._2.prezzoChiusuraIniziale)/tupla._2.prezzoChiusuraIniziale)*100) *100)/100;

            return result1;
        });
        // Ordino per la variazione della quotazione
        result = result.sortBy(r -> r.variazione, false, 1);

        result.saveAsTextFile(args[1]);
//        List<Result> collected = result.collect();
//        for(Result r : collected) {
//            System.out.println(r);
//        }

        spark.stop();


    }
}
