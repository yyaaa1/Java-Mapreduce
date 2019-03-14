import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class taskf {

    public static class map1
            extends Mapper<Object, Text, Text,Text>{
        private Text Bywho = new Text();
        private Text AccessTime = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] list_1 = (value.toString()).split(",");
            Bywho.set(list_1[1]);
            AccessTime.set(list_1[4]);
            context.write(Bywho, AccessTime);
        }
    }

    public static class reducer
            extends Reducer<Text,Text,Text, NullWritable> {

        public void reduce(Text key, Iterable<Text> values,
                           Context context) throws IOException, InterruptedException {
            Text ID = new Text();
            Text ID2 = new Text();
            int T = 1;
            for (Text v : values){
                if(Integer.parseInt(v.toString()) > 10){T++;
            }
        }
            if (T > 1) {
                ID.set(key);
                ID2.set(key);
            }
            if((ID2.toString()).length() > 0){
            context.write(ID, NullWritable.get());}
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "taskf");
        job.setJarByClass(taskf.class);
        job.setMapperClass(map1.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}