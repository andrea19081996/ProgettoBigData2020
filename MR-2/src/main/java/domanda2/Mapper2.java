package domanda2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Esegue la map del secondo job. Legge le righe su cui è stato già fatto il join ed emette coppie <Nome azienda, info>
 */
public class Mapper2 extends Mapper<LongWritable, Text, Text, Text> {

    Logger logger = LoggerFactory.getLogger(Mapper2.class);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split("\t");

        String ticker = parts[0];
        StringTokenizer st = new StringTokenizer(parts[1], ",");
        String nome = st.nextToken();
        String data = st.nextToken();
        String chiusura = st.nextToken();

        //logger.info("Emetto: " + nome + " " + ticker + "," + data + "," + chiusura );
        context.write(new Text(nome), new Text( ticker + "," + data + "," + chiusura ));
    }
}
