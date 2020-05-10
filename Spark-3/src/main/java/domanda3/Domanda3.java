package domanda3;


import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Serializable;
import scala.Tuple2;

import java.text.SimpleDateFormat;
import java.util.Date;


public class Domanda3 {

    public static class Stock implements Serializable {
        String ticker;
        String nome;
        Double chiusura;
        Date data;
    }

    public static class VarianzaPerTickerAnnoApp implements Serializable{
        String nome;
        Double prezzoIniziale;
        Double prezzoFinale;
        Date dataIniziale;
        Date dataFinale;
    }

    public static class VarianzaPerTickerAnno implements Serializable{
        String tickerAnno;
        String nome;
        Double variazione;
    }

    public static class TrendPerAzienda implements Serializable{
        String nome;
        Double var2016;
        Integer count2016;
        Double var2017;
        Integer count2017;
        Double var2018;
        Integer count2018;

    }

    private static Logger logger = LoggerFactory.getLogger(Domanda3.class);

    public static void main(String[] args) {
        if(args.length<3) {
            logger.error("Devi passare un parametro: percorso del file di input historical_stock_prices e file di input historical_stocks e percorso del file di output.");
            System.exit(-1);
        }
        SparkSession spark = SparkSession.builder().appName("Domanda 3").getOrCreate();
        JavaRDD<String> pricesLines = spark.read().textFile(args[0]).javaRDD();
        JavaRDD<String> stockesLines = spark.read().textFile(args[1]).javaRDD();

        pricesLines.filter(Domanda3::filterByYear);

        JavaPairRDD<String, String> prices = pricesLines.mapToPair(Domanda3::extractByTicker);
        JavaPairRDD<String, String> stocks = pricesLines.mapToPair(Domanda3::extractByTicker);

        JavaPairRDD<String, Tuple2<String, String>> joined = prices.join(stocks);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        JavaRDD<Stock> stocksDatas = joined.map(row -> {
            Stock result = new Stock();
            result.ticker = row._1;
            String[] parts = row._2._1.split(",");
            result.chiusura = Double.valueOf(parts[1]);
            result.data = sdf.parse(parts[6]);

            parts = row._2._2.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
            result.nome = parts[1];

            return result;
        });

        JavaPairRDD<String, VarianzaPerTickerAnnoApp> mapped = stocksDatas.mapToPair(stock -> {
            String key = stock.ticker + "-" + (stock.data.getYear() + 1900);
            VarianzaPerTickerAnnoApp var = new VarianzaPerTickerAnnoApp();
            var.nome=stock.nome;
            var.dataIniziale=stock.data;
            var.dataFinale=stock.data;
            var.prezzoIniziale=stock.chiusura;
            var.prezzoFinale=stock.chiusura;

            return new Tuple2<String, VarianzaPerTickerAnnoApp>(key, var);
        });

        JavaPairRDD<String, VarianzaPerTickerAnnoApp> reduced = mapped.reduceByKey((var1, var2) -> {
            VarianzaPerTickerAnnoApp result = new VarianzaPerTickerAnnoApp();

            result.nome = var1.nome;
            if (var1.dataIniziale.before(var2.dataIniziale)) {
                result.dataIniziale = var1.dataIniziale;
                result.prezzoIniziale = var1.prezzoIniziale;
            } else {
                result.dataIniziale = var2.dataIniziale;
                result.prezzoIniziale = var2.prezzoIniziale;
            }

            if (var1.dataFinale.after(var2.dataFinale)) {
                result.dataFinale = var1.dataFinale;
                result.prezzoFinale = var1.prezzoFinale;
            } else {
                result.dataFinale = var2.dataFinale;
                result.prezzoFinale = var2.prezzoFinale;
            }

            return result;
        });

        JavaRDD<VarianzaPerTickerAnno> variazioni = reduced.map(tupla -> {
            VarianzaPerTickerAnno var = new VarianzaPerTickerAnno();
            var.tickerAnno = tupla._1;
            var.nome = tupla._2.nome;
            var.variazione=((tupla._2.prezzoFinale - tupla._2.prezzoIniziale)/tupla._2.prezzoIniziale )*100;

            return var;
        });

        JavaPairRDD<String, TrendPerAzienda> mappedByAzienda = variazioni.mapToPair(var -> {
            TrendPerAzienda trends = new TrendPerAzienda();
            trends.nome = var.nome;
            trends.var2016=0.;
            trends.var2017=0.;
            trends.var2018=0.;
            trends.count2016=0;
            trends.count2017=0;
            trends.count2018=0;

            String anno = var.tickerAnno.split("-")[1];
            if(anno.equals("2016")) {
                trends.var2016 = var.variazione;
                trends.count2016=1;
            }else {
                if(anno.equals("2017")) {
                    trends.var2017 = var.variazione;
                    trends.count2017=1;
                }else {
                    trends.var2018 = var.variazione;
                    trends.count2018=1;
                }
            }

            return new Tuple2<>(var.nome, trends);
        });

        JavaPairRDD<String, TrendPerAzienda> firstResult = mappedByAzienda.reduceByKey((trend1, trend2) -> {
            TrendPerAzienda r = new TrendPerAzienda();
            r.var2016 = trend1.var2016 + trend2.var2016;
            r.count2016 = trend1.count2016 + trend2.count2016;
            r.var2017 = trend1.var2017 + trend2.var2017;
            r.count2017 = trend1.count2017 + trend2.count2017;
            r.var2018 = trend1.var2018 + trend2.var2018;
            r.count2018 = trend1.count2018 + trend2.count2018;
            r.nome = trend1.nome;

            return r;
        });

        JavaRDD<TrendPerAzienda> result = firstResult.map(tupla -> {
            TrendPerAzienda trend = tupla._2;
            trend.var2016 = trend.var2016/trend.count2016;
            trend.var2017 = trend.var2017/trend.count2017;
            trend.var2018 = trend.var2018/trend.count2018;

            return trend;
        });

        JavaPairRDD<String, String> mappedByTrend = result.mapToPair(t -> new Tuple2<>(t.var2016 + " // " + t.var2017 + " // " + t.var2018 , t.nome));
        JavaPairRDD<String, String> reducedBySameTrend = mappedByTrend.reduceByKey((a1, a2) -> a1 + " -- " + a2);

        reducedBySameTrend.saveAsTextFile(args[2]);
    }

    public static Tuple2<String, String> extractByTicker(String line) {
        String[] parts = line.split(",",2);
        return new Tuple2<String, String>(parts[0], parts[1]);
    }

    public static boolean filterByYear(String line) {
        String[] parts = line.split(",");
        String date= parts[7];  // Prendo la data dalla stringa
        date = date.substring(0,5);
        int year;
        try {
            year = Integer.parseInt(date);
        }catch(NumberFormatException e) {
            logger.error("Anno mal formattato. Riga scartata.");
            return false;
        }
        return year>=2016 && year<=2018;
    }
}
