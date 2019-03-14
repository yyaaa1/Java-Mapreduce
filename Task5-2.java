
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
//            extends Mapper<Object, Text, Text, Text>
            extends Mapper<Object, Text, Text, IntWritable>
    {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        Set set = new HashSet();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] valueArr = value.toString().split(",");
            StringBuilder stringBuilder = new StringBuilder(valueArr[1]);
            stringBuilder.append(' ');
            stringBuilder.append(valueArr[2]);
            String str = stringBuilder.toString();
            //put in set for cleanup process
            set.add(str);
        }

        protected void cleanup(Mapper<Object, Text, Text, IntWritable>.Context context)
                throws IOException, InterruptedException
        {
            Iterator it = set.iterator();
            while (it.hasNext())
            {
                Object nex = it.next();
                System.out.println(nex);
                // parse str to 2 part
                String [] strArr = nex.toString().split(" ");
                word.set(strArr[0]);
                context.write(word,one);

            }
        }
    }


    public static class IntSumReducer
                extends Reducer<Text, IntWritable, Text, IntWritable>
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

            result.set(sum);
            context.write(key, result);
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
