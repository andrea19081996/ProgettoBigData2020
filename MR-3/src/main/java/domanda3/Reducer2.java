package domanda3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Effettua la fase di reduce del secondo job. Quindi calcola, per ogni azienda, la variazione annua come media delle variazioni annue dei singoli ticker
 */
public class Reducer2 extends Reducer<Text, Text, Text, Text> {
    Logger logger = LoggerFactory.getLogger(Reducer2.class);

    private class DatiPerVarianzioneAnnua {
        public Date prima;
        public Double chiusuraIniziale;
        public Date ultima;
        public Double chiusuraFinale;
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String azienda=key.toString();
        Map<String, DatiPerVarianzioneAnnua> ticker2Dati = new HashMap();

        StringTokenizer st;

        String ticker;
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Date data = new Date();
        String anno="";
        Double chiusura;
        String chiave;

        for(Text value: values) {
//            logger.info("Analizzo: " + value.toString());
            st = new StringTokenizer(value.toString(), ",");
            ticker = st.nextToken();
            try {
                data = sdf.parse(st.nextToken());
                anno = String.valueOf(data.getYear() + 1900);
            } catch (ParseException e) {
                e.printStackTrace();
            }
            chiusura = Double.valueOf(st.nextToken());
            chiave = ticker + "-" + anno;

            // Aggiorno i dati riguardanti un ticker in un determinato anno
            DatiPerVarianzioneAnnua dati = ticker2Dati.get(chiave);
            if(dati == null) {
                dati = new DatiPerVarianzioneAnnua();
                dati.prima=data;
                dati.ultima=data;
                dati.chiusuraIniziale=chiusura;
                dati.chiusuraFinale=chiusura;
                ticker2Dati.put(chiave,dati);
            }

            if(data.before(dati.prima)) {
                dati.prima=data;
                dati.chiusuraIniziale=chiusura;
            }else {
                if(data.after(dati.ultima)) {
                    dati.ultima=data;
                    dati.chiusuraFinale=chiusura;
                }
            }
        }

        List<Double> variazione2016 = new LinkedList<>();
        List<Double> variazione2017 = new LinkedList<>();
        List<Double> variazione2018 = new LinkedList<>();

//        logger.info("AZIENDA: " + azienda);
        Set<String> chiavi = ticker2Dati.keySet();
//        for(String c : chiavi) {
//            logger.info("CHIAVE: " +c);
//        }

        for(String c : chiavi) {
            DatiPerVarianzioneAnnua dati = ticker2Dati.get(c);
            double var_quotazione= ((dati.chiusuraFinale - dati.chiusuraIniziale)/dati.chiusuraIniziale)*100;
            var_quotazione = (double) Math.round(var_quotazione * 100) / 100;
            String a = c.split("-")[1];
            if(a.equals("2016"))
                variazione2016.add(var_quotazione);
            else if(a.equals("2017"))
                variazione2017.add(var_quotazione);
            else
                variazione2018.add(var_quotazione);
        }

        // Costruisco il risultato: Azienda -> trend negli ultimi 3 anni
        Double somma=0.;
        StringBuilder result = new StringBuilder();
        for(Double val : variazione2016) {
            somma = somma + val;
        }
        if(variazione2016.size()!=0)
            result.append(Math.round( ( somma/((double)variazione2016.size()) ) *100) /100.0);

        somma=0.;
        for(Double val : variazione2017) {
            somma = somma + val;
        }
        result.append("-");
        if(variazione2017.size()!=0)
            result.append(Math.round( ( somma/((double)variazione2017.size()) ) *100) /100.0);

        somma=0.;
        for(Double val : variazione2018) {
            somma = somma + val;
        }
        result.append("-");
        if(variazione2018.size()!=0)
            result.append(Math.round( ( somma/((double)variazione2018.size()) ) *100) /100.0);

        context.write(new Text(azienda), new Text(result.toString()));

    }

}
