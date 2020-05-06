package domanda2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * Reducer del Job1: esegue il join.
 */
public class Reducer1 extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        List<String> toConcat = new LinkedList<>();
        String base="";
        String[] parts;

        for(Text value : values) {
            parts = value.toString().split(",");
            if(parts[0].equals("hs")) {
                base = parts[1];
            }else {
                // parts[0] == hsp
                toConcat.add(parts[1]+","+parts[2]);
            }
        }

        for(String s : toConcat) {
            context.write(key,new Text(base +","+ s));
        }
    }
}
