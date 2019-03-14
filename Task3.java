import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.util.*;
import java.lang.*;
public class Task1 {
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable>
    {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException
        {
            String[] valueArr = value.toString().split(",");
            word.set(valueArr[2]);
            context.write(word, one);
        }
    }
    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();
        Map<String,Integer> map=new HashMap<String, Integer>();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values)
            {
                sum += val.get();
            }
            String name = key.toString();
            map.put(name, sum);
        }
        protected void cleanup(Reducer<Text, IntWritable, Text, IntWritable>.Context context)
                throws IOException, InterruptedException
        {
            List<Map.Entry<String,Integer>> list = new LinkedList<Map.Entry<String,Integer>>(map.entrySet());
            Collections.sort(list,new Comparator<Map.Entry<String,Integer>>()
            {
                public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2)
                {
                    return (int) (o2.getValue()- o1.getValue());
                }
            });
            for(int i=0;i<10;i++)
            {
                context.write(new Text(list.get(i).getKey()), new IntWritable(list.get(i).getValue()));
            }
        }
    }
    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Task1");
        job.setJarByClass(Task1.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
