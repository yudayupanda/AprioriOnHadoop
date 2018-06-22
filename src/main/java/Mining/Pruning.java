package Mining;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import Mining.MiningkFIS.mapper;
import Mining.MiningkFIS.reducer;
import Util.Common;
import Util.HDFSFileUtils;


/**
 * 剪枝，限制候选项集的数目
 * @author Brother-Yu
 *
 */
public class Pruning {
	public static List<String> fisSet;
	public static int i = 1;

	public static class mapper extends Mapper<LongWritable, Text, Text, NullWritable> { 
		private Text k = new Text();
		public void setup(Context context) {	
			System.out.println("开始剪枝...");
			//读取频繁项集k
			String fisPath = "hdfs://192.168.178.131:9000/Apriori/FIMk/FIMk.txt";
			fisSet = new ArrayList<String>();
			fisSet = HDFSFileUtils.readFile2List(fisPath);
			System.out.println("开始剪枝1...");
		}
//		public void map(LongWritable key, Text value, Context context)  {
//			String line  = value.toString();
//			boolean isFrequent = Common.checkCandidateItemIsFrequent(fisSet,line);
//			System.out.println( line+isFrequent);
//		}
		
		public void map(LongWritable key, Text value, Context context)  {
			//读取每一行
			String line  = value.toString();
			//验证是否是频繁的
			boolean isFrequent = Common.checkCandidateItemIsFrequent(fisSet,line);
			System.out.println(line+isFrequent);
			if(isFrequent) {		
				k.set(line);
				try {
					context.write(k, NullWritable.get());
				} catch (IOException | InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}		
		}
		public void cleanup(Context context) {
			
		}
	}
	public static class reducer extends Reducer<Text, IntWritable, Text, NullWritable> {  
		public void reduce(Text key, Iterable<Text> values, Context context){ 
			try {
				context.write(key, NullWritable.get());	
			} catch (IOException | InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		public void cleanup(Context context) {
			
			System.out.println("剪枝结束");
		}
	}
//	public static Job getJob(String input,String output) throws IllegalArgumentException, IOException {
//		if(HDFSFileUtils.checkFileExist(output)) {
//			HDFSFileUtils.deleteFile(output);
//		}
//		Configuration conf = new Configuration();  
//	    Job job = new Job(conf);  
//	    job.setJarByClass(Pruning.class);  
//	    job.setJobName("Pruning");  
//  
//	    job.setMapperClass(mapper.class);  
//	    //job.setCombinerClass(reducer.class);
//	    job.setReducerClass(reducer.class);  
//	    //mapper输出的数据类型是什么？
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(NullWritable.class);
//	    FileInputFormat.addInputPath(job, new Path(input));  
//	    FileOutputFormat.setOutputPath(job, new Path(output));
//	    return job;
//	}

	public static int run(String input,String output) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
		if(HDFSFileUtils.checkFileExist(output)) {
			HDFSFileUtils.deleteFile(output);
		}
		Configuration conf = new Configuration();  
	    Job job = new Job(conf);  
	    job.setJarByClass(Pruning.class);  
	    job.setJobName("PruningTest");  
  
	    job.setMapperClass(mapper.class);  
	    //job.setCombinerClass(reducer.class);
	    job.setReducerClass(reducer.class);  
	    //mapper输出的数据类型是什么？
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
	    FileInputFormat.addInputPath(job, new Path(input));  
	    FileOutputFormat.setOutputPath(job, new Path(output));  
	    if(job.waitForCompletion(true)) {	
	    	String candidatePath = "hdfs://192.168.178.131:9000//Apriori//CandidateSet//";
	    	String deletedFilePath = candidatePath+"candidateSet.txt";
	    	HDFSFileUtils.deleteFile(deletedFilePath);
	    	//判断文件是否为空
	    	if(HDFSFileUtils.isFileEmpty(output+"//part-r-00000"))
	    		return 0;
	    	else {
	    		//保存为k频繁项集
		    	HDFSFileUtils.renameMV(output+"//part-r-00000", candidatePath+"//candidateSet.txt");
		    	HDFSFileUtils.deleteFile(output);
		    	return 1;
	    	}
	    
	    }else {
	    	return -1;
	    } 
	}
}
