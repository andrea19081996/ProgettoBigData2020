import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;


public class Job2MP extends Configured implements Tool {

    public static class MapperPrices extends Mapper<LongWritable, Text, Text, Text> {


        @Override
        protected void map(LongWritable key, Text value,
                           Context ctx) throws IOException, InterruptedException {
            String[] parts = value.toString().split(",");
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            Date maggiore = null;
            Date minore= null;
            Date current_data= null;
            try {
                maggiore = sdf.parse("2019-01-01");
                minore = sdf.parse("2007-12-31");
                current_data= sdf.parse(parts[7]);
                if(current_data.before(maggiore) && current_data.after(minore))
                    ctx.write(new Text(parts[0]), new Text("prices," + parts[2]+","+parts[6]+","+parts[7]));
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
    }

    public static class MapperStock extends Mapper<LongWritable, Text, Text, Text> {


        @Override
        protected void map(LongWritable key, Text value,
                           Context ctx) throws IOException, InterruptedException {
            String[] parts = value.toString().split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");

            ctx.write(new Text(parts[0]), new Text("stock," + parts[3]));
        }
    }

    public static class Reducer1 extends
            Reducer<Text, Text,Text,h_elementary_context> {


        @Override
        protected void reduce(Text key,
                              Iterable<Text> values, Context ctx)
                throws IOException, InterruptedException {

            Map<String, h_elementary> map_intermedia = new HashMap<String, h_elementary>();
            String sector_name = null;
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            Date data = null;
            h_elementary new_prices;

            List<String> valori_reduce = new ArrayList<String>();
            for (Text i : values) {
                valori_reduce.add(i.toString());
            }

            for (String i : valori_reduce) {
                String[] parts = i.split(",");
                if (parts[0].equals("stock"))
                    sector_name = parts[1];
            }

            for (String i : valori_reduce) {
                String[] parts = i.split(",");
                if (parts[0].equals("prices") && sector_name != null) {
                    String current_sector_year = sector_name + "-" + parts[3].split("-")[0];
                    if (!map_intermedia.containsKey(current_sector_year)) {
                        new_prices = new h_elementary();

                        try {
                            data = sdf.parse(parts[3]);
                            new_prices.setSector(sector_name);
                            new_prices.setData_max(data);
                            new_prices.setData_min(data);
                            new_prices.setPrezzo_chisura(Double.parseDouble(parts[1]));
                            new_prices.setPrezzo_chiusura_max(Double.parseDouble(parts[1]));
                            new_prices.setPrezzo_chiusura_min(Double.parseDouble(parts[1]));
                            new_prices.setVolume(Double.parseDouble(parts[2]));
                            new_prices.setContatore(1.0);

                            map_intermedia.put(current_sector_year, new_prices);
                        } catch (ParseException e) {
                            e.printStackTrace();
                        }


                    } else {
                        new_prices = map_intermedia.get(current_sector_year);

                        try {
                            data = sdf.parse(parts[3]);
                            if (data.before(new_prices.getData_min())) {
                                new_prices.setPrezzo_chiusura_min(Double.parseDouble(parts[1]));
                                new_prices.setData_min(data);
                            }

                            if (data.after(new_prices.getData_max())) {
                                new_prices.setPrezzo_chiusura_max(Double.parseDouble(parts[1]));
                                new_prices.setData_max(data);
                            }

                            new_prices.addContatore();
                            new_prices.addPrezzoChiusura(Double.parseDouble(parts[1]));
                            new_prices.addVolume(Double.parseDouble(parts[2]));

                            map_intermedia.put(current_sector_year, new_prices);
                        } catch (ParseException e) {
                            e.printStackTrace();
                        }

                    }

                }


            }

            Set<String> lista_chiavi = map_intermedia.keySet();
            for (String current_chiave : lista_chiavi) {
                h_elementary elemento_singolo = map_intermedia.get(current_chiave);
                h_elementary_context result = new h_elementary_context();
                result.setPrezzo_chisura(new DoubleWritable(elemento_singolo.getPrezzo_chisura()));
                result.setVolume(new DoubleWritable(elemento_singolo.getVolume()));
                result.setVariazione_annuale(new DoubleWritable((elemento_singolo.getPrezzo_chiusura_max()-elemento_singolo.getPrezzo_chiusura_min())/elemento_singolo.getPrezzo_chiusura_min()*100));

                result.setContatore(new DoubleWritable(elemento_singolo.getContatore()));


                ctx.write(new Text(current_chiave), result);
            }


        }

    }

    public static class Mapper2 extends Mapper<Text, Text, Text, h_elementary_context> {


        @Override
        protected void map(Text key, Text value,
                           Context ctx) throws IOException, InterruptedException {

            h_elementary_context current_element = new h_elementary_context();
            String[] parts=value.toString().split(",");

            current_element.setPrezzo_chisura(new DoubleWritable(Double.parseDouble(parts[1])));
            current_element.setVolume(new DoubleWritable(Double.parseDouble(parts[3])));
            current_element.setVariazione_annuale(new DoubleWritable(Double.parseDouble(parts[5])));
            current_element.setContatore(new DoubleWritable(Double.parseDouble(parts[7])));

            ctx.write(key, current_element);
        }
    }

    public static class Reducer2 extends
            Reducer<Text, h_elementary_context,Text,JoinResult> {


        @Override
        protected void reduce(Text key,
                              Iterable<h_elementary_context> values, Context ctx)
                throws IOException, InterruptedException {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

            double volume = 0.0;
            int contatore_volume_variazione=0;
            double contatore_quotazione=0.0;
            double prezzo_chiusura=0.0;
            double variazione_annuale=0.0;

            for(h_elementary_context current_elementary_context : values) {

                contatore_volume_variazione++;
                volume = volume + Double.parseDouble(current_elementary_context.getVolume().toString());
                contatore_quotazione = contatore_quotazione + Double.parseDouble(current_elementary_context.getContatore().toString());
                prezzo_chiusura = prezzo_chiusura + Double.parseDouble(current_elementary_context.getPrezzo_chisura().toString());
                variazione_annuale = variazione_annuale + Double.parseDouble(current_elementary_context.getVariazione_annuale().toString());

            }

            JoinResult result = new JoinResult();
            double volume_medio = volume/contatore_volume_variazione;
            result.setVolume_medio(new DoubleWritable(volume_medio));
            double quotazione_giornaliera_media = prezzo_chiusura/contatore_quotazione;
            result.setQuotazione_giornaliera_media(new DoubleWritable(quotazione_giornaliera_media));
            double variazione_annuale_finale= variazione_annuale/contatore_volume_variazione;
            result.setVariazione_annuale(new DoubleWritable(variazione_annuale_finale));

            ctx.write(key, result);
        }
    }

    public int run(String[] args) throws Exception {
        Path temp1 = new Path("temp");
        Path output = new Path(args[2]);
        Configuration conf = getConf();

        Job job2 = Job.getInstance(conf, "job2");
        MultipleInputs.addInputPath(job2, new Path(args[0]),TextInputFormat.class, MapperPrices.class);
        MultipleInputs.addInputPath(job2, new Path(args[1]),TextInputFormat.class, MapperStock.class);
        FileOutputFormat.setOutputPath(job2, temp1);
        //FileOutputFormat.setOutputPath(job2, output);

        job2.setJarByClass(Job2MP.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);

        job2.setReducerClass(Reducer1.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(h_elementary_context.class);

        job2.waitForCompletion(true);


        Job job3 = Job.getInstance(conf, "job3");
        FileInputFormat.setInputPaths(job3, temp1);
        FileOutputFormat.setOutputPath(job3, output);
        job3.setJarByClass(Job2MP.class);
        job3.setMapperClass(Mapper2.class);
        job3.setReducerClass(Reducer2.class);
        job3.setInputFormatClass(KeyValueTextInputFormat.class);
        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(h_elementary_context.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(JoinResult.class);
        job3.setNumReduceTasks(1);
        job3.waitForCompletion(true);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.out.println("Forza roma");
            System.exit(-1);
        }
        int res = ToolRunner.run(new Configuration(), new Job2MP(), args);
        System.exit(res);
    }
}
