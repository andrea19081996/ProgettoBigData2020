package domanda1;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.StringTokenizer;

public class Mapper2 extends
        Mapper<LongWritable, Text, Text, StatsResultWritable> {

    private Logger logger = LoggerFactory.getLogger(Mapper2.class);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split("\t",2);
        StringTokenizer st = new StringTokenizer(parts[1]);
        Text myKey = new Text(parts[0]);

        StatsResultWritable statsResultWritable = new StatsResultWritable();
        statsResultWritable.setVar_quotazione(new DoubleWritable(Double.parseDouble(st.nextToken())));
        statsResultWritable.setPrezzo_minimo(new DoubleWritable(Double.parseDouble(st.nextToken())));
        statsResultWritable.setPrezzo_massimo(new DoubleWritable(Double.parseDouble(st.nextToken())));
        statsResultWritable.setVolume_medio(new DoubleWritable(Double.parseDouble(st.nextToken())));
        context.write(myKey, statsResultWritable);
    }
}