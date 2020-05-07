package domanda3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Esegue la map del terzo job. Legge righe del tipo <Azienda> <Trend del triennio> e pubblica coppie invertite <Trend> <Azienda> per raggruppare le azienda per trend.
 */
public class Mapper3 extends Mapper<LongWritable, Text, Text, Text> {

    Logger logger = LoggerFactory.getLogger(Mapper3.class);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split("\t");
        context.write(new Text(parts[1]), new Text( parts[0] ));
    }
}
