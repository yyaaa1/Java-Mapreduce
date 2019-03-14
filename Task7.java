

import org.apache.commons.lang.ObjectUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.util.*;
import java.lang.*;


import java.io.*;
import java.util.concurrent.*;


public class Task1 {
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable>
    {
        private final static IntWritable num = new IntWritable(1);
        private Text word = new Text();
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            InputSplit inputSplit = context.getInputSplit();
            String fileName = ((FileSplit) inputSplit).getPath().getName();
            String[] valueArr = value.toString().split(",");
            String str =  null;
            if(fileName.equals("AccessLog.csv"))
            {
                StringBuilder stringBuilder = new StringBuilder(valueArr[2]);
                stringBuilder.append(',');
                stringBuilder.append(valueArr[1]);
                str = stringBuilder.toString();
                num.set(-99999);
            }

            if(fileName.equals("Friends.csv"))
            {
                StringBuilder stringBuilder = new StringBuilder(valueArr[1]);
                stringBuilder.append(',');
                stringBuilder.append(valueArr[2]);
                str = stringBuilder.toString();
                num.set(1);
            }
            word.set(str); // for q1
            context.write(word, num);
        }
    }

    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, NullWritable>
    {
        private IntWritable result = new IntWritable();
        Map<String, Integer> map = new HashMap<String, Integer>();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException
        {
            int sum = 0;
            for (IntWritable val : values)
            {
                sum += val.get();
            }
            String name = key.toString();
            if(sum>0)
            {
                context.write(key, NullWritable.get());
            }
        }
    }
    public static void main(String[] args) throws Exception
    {

        long startTime = System.nanoTime();

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Task1");
        job.setJarByClass(Task1.class);
        job.setMapperClass(TokenizerMapper.class);

        job.setReducerClass(IntSumReducer.class);
        job.setNumReduceTasks(1);//set number of reducer to 1
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
