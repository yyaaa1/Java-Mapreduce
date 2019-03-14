import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.commons.lang3.StringUtils;


public class Task_d_Combiner {

    public static class map1
            extends Mapper<Object, Text, Text, Text> {
        private Text Id = new Text();
        private Text Name = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

//            InputSplit inputSplit = context.getInputSplit();
//            String fileName = ((FileSplit) inputSplit).getPath().getName();
//            System.out.println(fileName);
            String[] list_1 = value.toString().split(",");
            Id.set(list_1[0]);
            String Name_1 = "0"+ list_1[1];
            Name.set(Name_1);
            context.write(Id, Name);
        }
    }

    public static class map2
            extends Mapper<Object, Text, Text, Text> {
        private Text Id = new Text();
        private Text Mf = new Text("1");

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] list_1 = value.toString().split(",");
            Id.set(list_1[2]);
            context.write(Id, Mf);
        }
    }


    public static class Combiner
            extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            Text NoF = new Text();
            String a2 = new String();
            int i = 0;
            for (Text v : values) {
                char a = v.toString().charAt(0);
                String a_s = String.valueOf(a);
                if (a_s.equals("1")) {
                    i++;
                }
                else {a2 = v.toString().substring(1);
                }
            }
            String str1 = String.valueOf(i);
            if (StringUtils.isNotBlank(a2)){
            NoF.set(a2+","+str1);}
            else{
                NoF.set(str1);}

            context.write(key, NoF);
        }
    }



    public static class Reducer1
            extends Reducer<Text, Text, Text, IntWritable> {
        private IntWritable NoF = new IntWritable();
        private Text Name = new Text();
        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            int i = 0;
            int j = 0;
            for (Text v : values) {
                if(v.toString().contains(",")){
                    String[] g = v.toString().split(",");
                    Name.set(g[0]);
                    j = j+Integer.parseInt(g[1]);
                }
                else{i = i+Integer.parseInt(v.toString());

                }
            }
            i = i+j;
            NoF.set(i);
            context.write(Name, NoF);
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "taskd");
        Path p1=new Path(args[0]);
        Path p2=new Path(args[1]);
        Path p3=new Path(args[2]);
        job.setJarByClass(Task_d_Combiner.class);
        job.setMapOutputValueClass(Text.class);
        job.setCombinerClass(Combiner.class);
        job.setReducerClass(Reducer1.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        MultipleInputs.addInputPath(job, p1, TextInputFormat.class, map1.class);
        MultipleInputs.addInputPath(job, p2, TextInputFormat.class, map2.class);
        FileOutputFormat.setOutputPath(job, p3);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


}

