import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import jdk.internal.util.xml.impl.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class CountTopTen {
    private String sourcePath;
    private String outputPath;
    private Configuration conf;

    public CountTopTen(String sourcePath, String outputPath, Configuration conf){
        this.sourcePath = sourcePath;
        this.outputPath = outputPath;
        this.conf = conf;
    }
    public void SumJob() throws IOException, InterruptedException, ClassNotFoundException{
        Job SumJob = new Job();
        SumJob.setJobName("SumJob");
        SumJob.setJarByClass(CountTopTen.class);
        SumJob.getConfiguration().set("SumPath", outputPath + "/sumtemp");

        SumJob.setMapperClass(CountTopTen.TokenizerMapper.class);
        SumJob.setMapOutputKeyClass(Text.class);
        SumJob.setMapOutputValueClass(IntWritable.class);

//        SumJob.setCombinerClass(CountTopTen.SumCombiner.class);
//        SumJob.setPartitionerClass(CountTopTen.SumPartition.class);
        SumJob.setReducerClass(CountTopTen.IntSumReducer .class);
        SumJob.setOutputKeyClass(Text.class);
        SumJob.setOutputValueClass(IntSumReducer.class);
        FileInputFormat.addInputPath(SumJob, new Path(sourcePath));
        FileOutputFormat.setOutputPath(SumJob, new Path(outputPath + "/sumtemp"));

        SumJob.waitForCompletion(true);
        System.out.println("finished!");
    }

    public void SortJob() throws IOException, InterruptedException, ClassNotFoundException{
        Job SortJob = new Job();
        SortJob.setJobName("SortJob");
        SortJob.setJarByClass(CountTopTen.class);
        SortJob.getConfiguration().set("SortPath", outputPath + "/sort");

        SortJob.setMapperClass(CountTopTen.SortMapper.class);
        SortJob.setMapOutputKeyClass(Text.class);
        SortJob.setMapOutputValueClass(Text.class);

        SortJob.setReducerClass(CountTopTen.SortReducer .class);
        SortJob.setOutputKeyClass(Text.class);
        SortJob.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(SortJob, new Path(outputPath+"/sumtemp"));
        FileOutputFormat.setOutputPath(SortJob, new Path(outputPath + "/sort"));

        SortJob.waitForCompletion(true);
        System.out.println("finished!");
    }

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] line = value.toString().split(",");
            word.set(line[1]+","+line[10]);
            context.write(word, one);
        }
    }

//    public  static class SumCombiner
//            extends Reducer<Text, IntWritable, Text, IntWritable> {
//        private IntWritable result = new IntWritable();
//        public void reduce(Text key, Iterable<IntWritable> values,
//                           Context context
//        ) throws IOException, InterruptedException {
//            int sum = 0;
//            for (IntWritable val : values) {
//                sum += val.get();
//            }
//            result.set(sum);
//            context.write(key,result);
//
//        }
//    }
//    public static class SumPartition
//        extends Partitioner<Text,IntWritable>{
//        @Override
//        public int getPartition(Text k, IntWritable v, int i) {
//            String[] key = k.toString().split(",");
//            return (key[1].hashCode()&Integer.MAX_VALUE)%i;
//        }
//    }

    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();
        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key,result);

        }
    }

    public static class SortMapper
            extends Mapper<Object, Text, Text, Text> {

        private final static IntWritable one = new IntWritable(1);
        private IntWritable word = new IntWritable();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] line = value.toString().split("\t");
            String[] preKey = line[0].split(",");
            Text temp1 = new Text();
            Text temp2 = new Text();
            System.out.println(line[0]);
            System.out.println(line[1]);
            temp1.set(line[1]+","+preKey[0]);
            temp2.set(preKey[1]);
            context.write(temp2, temp1);//temp2 is province temp1 is num+ID
        }
    }

    public static class SortReducer
            extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            ArrayList<Integer> num = new ArrayList<>();
            ArrayList<String> ProandID = new ArrayList<>();
            int judge=0;
            for (Text item :values){
                String[] temp=item.toString().split(",");
                if(temp.length<2) {
                    judge=1;
                    break;
                }
                num.add(Integer.valueOf(temp[0]));
                String tempPD=key+temp[1];
                ProandID.add(tempPD);
//                context.write(new Text(item.toString()),new Text());
            }
            if(judge==0) {
                for (int i = 0; i < num.size(); i++) {
                    for (int j = i + 1; j < num.size(); j++) {
                        if (num.get(j) > num.get(i)) {
                            String temp = ProandID.get(i);
                            Integer numtemp = num.get(i);
                            num.set(i, num.get(j));
                            ProandID.set(i, ProandID.get(j));
                            num.set(j, numtemp);
                            ProandID.set(j, temp);
                        }
                    }
                }
                if (num.size() >= 10) {
                    for (int i = 0; i < 10; i++) {
                        context.write(new Text(ProandID.get(i)), new Text(String.valueOf(num.get(i))));
                    }
                } else {
                    for (int i = 0; i < num.size(); i++) {
                        context.write(new Text(ProandID.get(i)), new Text(String.valueOf(num.get(i))));
                    }
                }
            }
        }
    }




    public static void main(String [] args) throws Exception {
        Configuration conf = new Configuration();
        String sourcePath = args[0];
        String outputPath = args[1];
        CountTopTen driver = new CountTopTen(sourcePath, outputPath, conf);
        driver.SumJob();
        driver.SortJob();


//        job.setJarByClass(WordCount.class);
//        job.setMapperClass(SortMapper.class);
//        job.setCombinerClass(SortReducer.class);
//        job.setReducerClass(SortReducer.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(IntWritable.class);
//        FileInputFormat.addInputPath(job, new Path(args[1]+"/sumtemp"));
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));
//        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
