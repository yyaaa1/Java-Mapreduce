import java.io.IOException;
import java.util.*;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class taskh {

    public static class map1
            extends Mapper<Object, Text, Text, IntWritable> {
        private IntWritable one = new IntWritable(1);

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            Text Name = new Text();
            Name.set("MyPage");
            context.write(Name,one);
        }
    }

    public static class map2
            extends Mapper<Object, Text, Text, IntWritable> {
        private Text Id = new Text();
        private IntWritable input = new IntWritable(1);

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] list_1 = value.toString().split(",");
            Id.set(list_1[2]);
            context.write(Id, input);
        }
    }



    public static class Reducer1
            extends Reducer<Text,IntWritable,Text,NullWritable> {
        private static IntWritable totalpeople = new IntWritable();
        private static IntWritable averagefriend = new IntWritable();
        private static String allsum = new String();
        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            if(key.toString().equals("MyPage")){
                int sum = 0;
                for (IntWritable v : values) {
                    sum += v.get(); }
                totalpeople.set(sum);}
            else{
                int sum2 = 0;
                for (IntWritable v : values) {
                    sum2 += v.get(); }
                allsum = allsum + key.toString()+" "+sum2 +",";
            }

        }
        protected void cleanup(Context context) throws IOException,
                InterruptedException {
            Text IDout = new Text();
            int totalR = 0;
            String[] list_1 = allsum.split(",");
            for(String j:list_1){
                String[] list_3 = j.split(" ");
                totalR = totalR + Integer.parseInt(list_3[1]);
            }
            float a = totalR/totalpeople.get();
            for (String i:list_1){
                String[] list_2 = i.split(" ");
                if (Integer.parseInt(list_2[1])>a){
                    IDout.set(list_2[0]);
                    context.write(IDout,NullWritable.get());
                    }


                }
            }

        }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "taskh");
        Path p1=new Path(args[0]);
        Path p2=new Path(args[1]);
        Path p3=new Path(args[2]);
        job.setJarByClass(taskh.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setReducerClass(Reducer1.class);
        job.setNumReduceTasks(1);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        MultipleInputs.addInputPath(job, p1, TextInputFormat.class, map1.class);
        MultipleInputs.addInputPath(job, p2, TextInputFormat.class, map2.class);
        FileOutputFormat.setOutputPath(job, p3);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


}

