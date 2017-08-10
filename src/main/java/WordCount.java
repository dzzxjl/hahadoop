/**
 * Created by dzzxjl on 2017/7/7.
 */
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

    /*
    Mapper类
     */
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {
        // final关键字定义1？？？
        private final static IntWritable one = new IntWritable(1);

        private Text word = new Text();

        //map函数
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            // 按行读取？
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
                System.out.println(word);
            }
        }
    }


    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        //reduce函数
        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        // 加载配置
        Configuration conf = new Configuration();
        // 使用单例模式创建一个名为word count的job
        Job job = Job.getInstance(conf, "word count");
        // 加载本类
        job.setJarByClass(WordCount.class);
        //  加载Map模型
        job.setMapperClass(TokenizerMapper.class);
        // 加载Reduce模型
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        // 设置输出类
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);


        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
