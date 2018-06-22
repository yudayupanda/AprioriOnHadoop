package Mining;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import Util.Common;
import Util.HDFSFileUtils;


public class GetMinSupport {
	public static int count = 0;
	public static class mapper extends Mapper<LongWritable, Text, Text, IntWritable> { 
		private final IntWritable one = new IntWritable(1);  
		private Text k = new Text();
		public void setup(Context context) {
			System.out.println("开始计算支持度计数");
		}
		public void map(LongWritable key, Text value, Context context)  {	
			try {
					System.out.println(value.toString());
					count++;
					k.set(value.toString());
					context.write(k, one);

			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	public static class reducer extends Reducer<Text, IntWritable, NullWritable, NullWritable> {  
		public void reduce(Text key, Iterable<IntWritable> values, Context context){  
			try {
				context.write(NullWritable.get(), NullWritable.get());
			} catch (IOException | InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	public static int run(String input,String output,double support) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
		if(HDFSFileUtils.checkFileExist(output)) {
			HDFSFileUtils.deleteFile(output);
		}
		Configuration conf = new Configuration();  
	    Job job = new Job(conf);  
	    job.setJarByClass(Mining1FIS.class);  
	    job.setJobName("Initialize");  
  
	    job.setMapperClass(mapper.class);  
	    job.setReducerClass(reducer.class);  
	    //mapper输出的数据类型是什么？
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job, new Path(input));  
	    FileOutputFormat.setOutputPath(job, new Path(output));  
	    if(job.waitForCompletion(true)) {
	    
	    	return (int) (count * support);
	    	
	    }else {
	    	return -1;
	    }
	//  return job.waitForCompletion(true);  
	}
}
