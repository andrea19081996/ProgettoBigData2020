import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.regex.Pattern;

public class Job1_Spark {
    private static final Pattern Comma = Pattern.compile(",");

    public static void main(String[] args) throws Exception {


        //creazione Spark Session
        SparkConf conf = new SparkConf()
                .setAppName("Job1_Spark")
                .setMaster("local[*]");

        JavaSparkContext sc= new JavaSparkContext(conf);


        SimpleDateFormat sdf= new SimpleDateFormat("yyyy-MM-dd");

        JavaPairRDD<String,String> v_stoc_prices= sc.textFile(args[0]).filter(ele -> !ele.split(",")[0].equals("ticker") && sdf.parse(ele.split(",")[7]).getYear()+1900>2007 && sdf.parse(ele.split(",")[7]).getYear()+1900<2019).mapToPair(f -> new Tuple2<>(f.split(",")[0], f.split(",")[2]+","+f.split(",")[6]+","+f.split(",")[7]));
        JavaPairRDD<String,String> v_stoc= sc.textFile(args[1]).filter(ele -> !ele.split(",")[0].equals("ticker")).mapToPair(f -> new Tuple2<>(f.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)")[0], f.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)")[3]));
        JavaPairRDD<String, Tuple2<String, String>> v_join = v_stoc_prices.join(v_stoc);
        JavaRDD<h_elementary> join_stock= v_join.map(Job1_Spark::crea_oggetto_singolo);

        JavaPairRDD<String, h_elementary> pair_join_stock = join_stock.mapToPair(ele -> new Tuple2<>(ele.getSector()+"-"+ele.getData_max().toString().split(" ")[5]+"-"+ele.getTicker(), ele));

        pair_join_stock= pair_join_stock.reduceByKey(Job1_Spark::risultato);


        JavaRDD<JoinResult> join_result = pair_join_stock.map(Job1_Spark::estrapola);

        JavaPairRDD<String, JoinResult> join_result_pair = join_result.mapToPair(ele -> new Tuple2<>(ele.getSector_year(),ele));

        join_result_pair = join_result_pair.reduceByKey(Job1_Spark::aggrega_settore_anno);
        JavaRDD<h_sector_year_aggregation> result = join_result_pair.map(Job1_Spark::crea_result);

        result.saveAsTextFile(args[2]);

    }

    private static h_elementary crea_oggetto_singolo(Tuple2<String, Tuple2<String, String>> f) throws ParseException {
        h_elementary result = new h_elementary();
        SimpleDateFormat sdf= new SimpleDateFormat("yyyy-MM-dd");
        String[] split = f._2._1.split(",");

        result.setTicker(f._1);
        result.setSector(f._2._2);
        result.setPrezzo_chisura(Double.parseDouble(split[0]));
        result.setPrezzo_chiusura_min(Double.parseDouble(split[0]));
        result.setPrezzo_chiusura_max(Double.parseDouble(split[0]));
        result.setContatore(1.0);
        result.setVolume(Double.parseDouble(split[1]));
        result.setData_min(sdf.parse(split[2]));
        result.setData_max(sdf.parse(split[2]));

        return result;
    }

    private static h_sector_year_aggregation crea_result(Tuple2<String,JoinResult> f1) {
        h_sector_year_aggregation result = new h_sector_year_aggregation();

        JoinResult current_element = f1._2;
        result.setSector_year(current_element.getSector_year());
        result.setVolume_medio(current_element.getVolume_medio()/ current_element.getContatore_variazione_volume());
        result.setVariazione_annuale(current_element.getVariazione_annuale()/ current_element.getContatore_variazione_volume());
        result.setQuotazione_giornaliera_media(current_element.getQuotazione_giornaliera()/ current_element.getContatoreQuotazione());

        return result;
    }

    private static JoinResult aggrega_settore_anno(JoinResult f1, JoinResult f2) {
        JoinResult result = new JoinResult();

        result.setSector_year(f1.getSector_year());
        result.setQuotazione_giornaliera(f1.getQuotazione_giornaliera()+f2.getQuotazione_giornaliera());
        result.setVolume_medio(f1.getVolume_medio()+f2.getVolume_medio());
        result.setVariazione_annuale(f1.getVariazione_annuale()+f2.getVariazione_annuale());

        result.setContatore_variazione_volume(f1.getContatore_variazione_volume()+f2.getContatore_variazione_volume());
        result.setContatoreQuotazione(f1.getContatoreQuotazione()+f2.getContatoreQuotazione());

        return result;
    }


    private static JoinResult estrapola(Tuple2<String, h_elementary> f) {
        JoinResult result = new JoinResult();
        String [] parts=f._1.split("-");
        result.setSector_year(parts[0]+"-"+parts[1]);
        double contatore = f._2.getContatore();
        result.setVolume_medio(f._2.getVolume());
        result.setQuotazione_giornaliera(f._2.getPrezzo_chisura());
        result.setVariazione_annuale((f._2.getPrezzo_chiusura_max()-f._2.getPrezzo_chiusura_min())/f._2.getPrezzo_chiusura_min()*100);
        result.setContatore_variazione_volume(1);
        result.setContatoreQuotazione(contatore);

        return result;
    }

    private static h_elementary risultato(h_elementary el1, h_elementary el2) {
        h_elementary result = new h_elementary();

        result.setSector(el1.getSector());

        //PER PREZZO MASSIMO-caso el1 data più avanti di el2
        if (el1.getData_max().compareTo(el2.getData_max()) > 0) {
            result.setData_max(el1.data_max);
            result.setPrezzo_chiusura_max(el1.getPrezzo_chiusura_max());
        } else if (el1.getData_max().compareTo(el2.getData_max()) < 0) {
            result.setData_max(el2.data_max);
            result.setPrezzo_chiusura_max(el2.getPrezzo_chiusura_max());
            //caso uguali date massime
            //chiedere opinione davide
        } else {
            result.setData_max(el1.data_max);
            result.setPrezzo_chiusura_max(el1.getPrezzo_chiusura_max());
        }

        //PER PREZZO MINIMO-caso el1 data più avanti di el2
        if (el1.getData_min().compareTo(el2.getData_max()) > 0) {
            result.setData_min(el2.data_min);
            result.setPrezzo_chiusura_min(el2.getPrezzo_chiusura_min());
        } else if (el1.getData_max().compareTo(el2.getData_max()) < 0) {
            result.setData_min(el1.data_min);
            result.setPrezzo_chiusura_min(el1.getPrezzo_chiusura_min());
            //caso uguali date minime
            //chiedere opinione davide
        } else {
            result.setData_min(el1.data_min);
            result.setPrezzo_chiusura_min(el1.getPrezzo_chiusura_min());
        }

        result.setTicker(el1.getTicker());
        result.setVolume(el1.getVolume() + el2.getVolume());
        result.setPrezzo_chisura(el1.getPrezzo_chisura() + el2.getPrezzo_chisura());
        result.setContatore(el1.getContatore() + el2.getContatore());

        return result;
    }

}
