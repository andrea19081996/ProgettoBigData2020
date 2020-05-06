package domanda1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Job extends Configured implements Tool {

    private Logger logger = LoggerFactory.getLogger(Job.class);

    public int run(String[] args) throws Exception {
        Path input1= new Path(args[0]);
        Path output = new Path(args[1]);
        Path temp = new Path("output/temp");
        Configuration conf = getConf();

        org.apache.hadoop.mapreduce.Job job1 = org.apache.hadoop.mapreduce.Job.getInstance(conf, "job1");
        FileInputFormat.addInputPath(job1, input1);
        FileOutputFormat.setOutputPath(job1, temp);

        job1.setJarByClass(Job.class);
        job1.setMapperClass(Mapper1.class);
        job1.setReducerClass(Reducer1.class);
        job1.setNumReduceTasks(Integer.parseInt(args[2]));

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(StockWritable.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(StatsResultWritable.class);
        boolean s = job1.waitForCompletion(true);
        if(!s) {
            logger.error("Errore nell'esecuzione del job1");
            return -1;
        }

        Configuration conf2 = getConf();

        org.apache.hadoop.mapreduce.Job job2 = org.apache.hadoop.mapreduce.Job.getInstance(conf2, "job2");
        FileInputFormat.addInputPath(job2, temp);
        FileOutputFormat.setOutputPath(job2, output);

        job2.setJarByClass(Job.class);
        job2.setMapperClass(Mapper2.class);
        job2.setReducerClass(Reducer2.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(StatsResultWritable.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(StatsResultWritable.class);
        s=job2.waitForCompletion(true);
        if(!s) {
            logger.error("Errore nell'esecuzione del job2");
            return -1;
        }
        return 0;
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.out.println("Numero di parametri errato. Parametri: <file di input> <directory di output> <num reducer intermedi>");
            System.exit(-1);
        }
        int res = ToolRunner.run(new Configuration(), new Job(), args);
        System.exit(res);
    }
}