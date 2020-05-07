package domanda3;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Job extends Configured implements Tool {

    private Logger logger = LoggerFactory.getLogger(Job.class);

    public int run(String[] args) throws Exception {
        Path hspPath= new Path(args[0]);
        Path hsPath = new Path(args[1]);
        Path temp1 = new Path("output/temp1");
        Path temp2 = new Path("output/temp2");
        Path output = new Path(args[2]);
        Configuration conf = getConf();

        org.apache.hadoop.mapreduce.Job job1 = org.apache.hadoop.mapreduce.Job.getInstance(conf, "Domanda 3 Join");
        job1.setJarByClass(Job.class);

        job1.setReducerClass(Reducer1.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job1, hsPath, TextInputFormat.class, Mapper1HS.class);
        MultipleInputs.addInputPath(job1, hspPath, TextInputFormat.class, Mapper1HSP.class);
        FileOutputFormat.setOutputPath(job1, temp1);

        boolean s = job1.waitForCompletion(true);
        if(!s) {
            logger.error("Errore nell'esecuzione del job1");
            return -1;
        }

        Configuration conf2 = getConf();
        org.apache.hadoop.mapreduce.Job job2 = org.apache.hadoop.mapreduce.Job.getInstance(conf2, "Domanda 3 Secondo job");
        job2.setJarByClass(Job.class);

        job2.setReducerClass(Reducer2.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        job2.setMapperClass(Mapper2.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job2, temp1);
        FileOutputFormat.setOutputPath(job2, temp2);

        s = job2.waitForCompletion(true);
        if(!s) {
            logger.error("Errore nell'esecuzione del job2");
            return -1;
        }

        Configuration conf3 = getConf();
        org.apache.hadoop.mapreduce.Job job3 = org.apache.hadoop.mapreduce.Job.getInstance(conf3, "Domanda 3 Terzo job");
        job3.setJarByClass(Job.class);

        job3.setReducerClass(Reducer3.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);

        job3.setMapperClass(Mapper3.class);
        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job3, temp2);
        FileOutputFormat.setOutputPath(job3, output);

        s = job3.waitForCompletion(true);
        if(!s) {
            logger.error("Errore nell'esecuzione del job3");
            return -1;
        }

        return 0;

    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.out.println("Numero di parametri errato. Parametri: <file historical stock prices> <file historical stock> <directory di output> ");
            System.exit(-1);
        }
        int res = ToolRunner.run(new Configuration(), new Job(), args);
        System.exit(res);
    }
}