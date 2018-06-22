package Mining;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.commons.lang.StringUtils;
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

/**
 * 自连接:产生候选项集
 * @author Brother-Yu
 *
 */
public class SelfConnection {

	public static int i = 1;
	public static class mapper extends Mapper<LongWritable, Text, Text, Text> {
		private Text k = new Text();
		private Text v = new Text();
		public void setup(Context context) {
			System.out.println("开始产生候选项集...");
		
		}
		public void map(LongWritable key, Text value, Context context)  {
		//	System.out.println("第"+(i)+"次");
			String line = value.toString();
			//一行频繁项集
			List<String> fisLine =  new ArrayList<String>();
			StringTokenizer token = new StringTokenizer(line);  
		    while (token.hasMoreTokens()) { 
		    	String item = token.nextToken();
		    	fisLine.add(item);
		    }
			int size = fisLine.size();
		    String lastItem = fisLine.get(size-1);
		    fisLine.remove(size-1);
	    	try {
	    		String keyString =  StringUtils.join(fisLine.toArray(), " ");
//	    		System.out.println("前半部分："+keyString);
//	    		System.out.println("后半部分："+lastItem);
	    		k.set(keyString);
	    		v.set(lastItem);
				context.write(k, v);
				
			} catch (IOException | InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

	
		}
	}
	public static class reducer extends Reducer<Text, Text, Text, NullWritable> { 
		private Text k = new Text();
		
		public void reduce(Text key, Iterable<Text> values, Context context){  
			try {
				List<String> lastHalfItems = new ArrayList<String>();
				for(Text v:values) {
					lastHalfItems.add(v.toString());
				}
				List<String> connectionResult = Common.lastItemConnect(lastHalfItems);
				for(String last:connectionResult) {
					String first = key.toString();
					k.set(first+" "+last);
					System.out.println("候选项集:"+first+" "+last);
					context.write(k, NullWritable.get());
				}
				
			} catch (IOException | InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		public void cleanup(Context context) {
				System.out.println("产生候选项集结束...");
		}
	}
	public static Job getJob(String input,String output) throws IllegalArgumentException, IOException {
		if(HDFSFileUtils.checkFileExist(output)) {
			HDFSFileUtils.deleteFile(output);
		}
		Configuration conf = new Configuration();  
	    Job job = new Job(conf);  
	    job.setJarByClass(MiningkFIS.class);  
	    job.setJobName("SelfConnection");  
  
	    job.setMapperClass(mapper.class);  
	//    job.setCombinerClass(reducer.class);
	    job.setReducerClass(reducer.class);  
	    //mapper输出的数据类型是什么？
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job, new Path(input));  
	    FileOutputFormat.setOutputPath(job, new Path(output)); 
	    return job;
	}
	public static int  run(String input,String output) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
		if(HDFSFileUtils.checkFileExist(output)) {
			HDFSFileUtils.deleteFile(output);
		}
		Configuration conf = new Configuration();  
	    Job job = new Job(conf);  
	    job.setJarByClass(MiningkFIS.class);  
	    job.setJobName("SelfConnection");  
  
	    job.setMapperClass(mapper.class);  
	//    job.setCombinerClass(reducer.class);
	    job.setReducerClass(reducer.class);  
	    //mapper输出的数据类型是什么？
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job, new Path(input));  
	    FileOutputFormat.setOutputPath(job, new Path(output));  
	    if(job.waitForCompletion(true)) {
	    
	    	if(HDFSFileUtils.isFileEmpty(output+"//part-r-00000")) {
	    		System.out.println("剪枝：在候选项集生成中结束！！！");
	    		return 0;
	    	}
	    	HDFSFileUtils.renameMV(output+"//part-r-00000", output+"//candidateSet.txt");
	    	return 1;
//	    	else {
//	    		//保存为k候选项集
//		    	HDFSFileUtils.renameMV(output+"//part-r-00000", output+"//CandidateSet.txt");
//		    	return 1;
//	    	}
	    }else {
	    	return -1;
	    }
		
	}
}
