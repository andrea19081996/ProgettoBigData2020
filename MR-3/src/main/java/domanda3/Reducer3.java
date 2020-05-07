package domanda3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Effettua la fase di reduce del terzo job. Concatena i nomi delle aziende con lo tesso trend.
 */
public class Reducer3 extends Reducer<Text, Text, Text, Text> {
    Logger logger = LoggerFactory.getLogger(Reducer3.class);

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder sb = new StringBuilder();
        sb.append("Aziende: ");
        int count =0;

        if(values.iterator().hasNext()) {
            sb.append(values.iterator().next().toString());
            count++;
        }

        for(Text value : values) {
            sb.append(", ");
            sb.append(value.toString());
            count++;
        }

        if(count>1)
            context.write(key, new Text(sb.toString()));

    }

}
