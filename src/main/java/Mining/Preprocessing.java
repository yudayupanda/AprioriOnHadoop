package Mining;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

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

import Util.HDFSFileUtils;

/**
 * 预处理：每行事务按照数字大小排序
 * @author Brother-Yu
 *
 */
public class Preprocessing {
	public static Long index  = 0L;
	public static class mapper extends Mapper<LongWritable, Text,  LongWritable,Text> { 
		public void setup(Context context) {
			System.out.println("开始预处理");
		}
		private Text v = new Text();
		private LongWritable k = new LongWritable();
		public void map(LongWritable key, Text value, Context context)  {
			String line = value.toString();
//			List<Integer> list = new ArrayList<Integer>();
//		    StringTokenizer token = new StringTokenizer(line);  
//		    while (token.hasMoreTokens()) { 
//		    	list.add(Integer.parseInt(token.nextToken()));
//		    }
//		    Collections.sort(list);
//		    StringBuffer sBuffer = new StringBuffer();
//		    list.forEach(s -> sBuffer.append(s+" "));  
//		    String str = sBuffer.toString();
//		    v.set(str);
			v.set(line);
		    try {
		    	k.set(index++);
		    	if(line.trim().length()!=0)
		    		context.write(k,v);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	public static class reducer extends Reducer<LongWritable,Text, NullWritable,Text> {  
		public void reduce(LongWritable key, Iterable<Text> values, Context context){  
			try {
				  for (Text val : values) {  
					  context.write(NullWritable.get(), val); 
				    } 
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		public void cleanup(Context context) {
		
				System.out.println("预处理结束");

		}
	}
	public static boolean run(String input,String output) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
		if(HDFSFileUtils.checkFileExist(output)) {
			HDFSFileUtils.deleteFile(output);
		}
		Configuration conf = new Configuration();  
	    Job job = new Job(conf);  
	    job.setJarByClass(Mining1FIS.class);  
	    job.setJobName("Preprocessing");  
  
	    job.setMapperClass(mapper.class);  
	    job.setReducerClass(reducer.class);  
	    //mapper输出的数据类型是什么？
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job, new Path(input));  
	    FileOutputFormat.setOutputPath(job, new Path(output));  
	    if(job.waitForCompletion(true)) {
	    	
	    	HDFSFileUtils.renameMV(output+"//part-r-00000", output+"//data.txt");
	    	System.out.println("预处理完毕");
	    	return true;
	    }else {
	    	return false;
	    }
	//  return job.waitForCompletion(true);  
	}
}
