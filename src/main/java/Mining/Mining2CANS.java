package Mining;

import java.io.IOException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
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
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


import Util.Common;
import Util.HDFSFileUtils;
/**
 * 挖掘2候选项集
 * @author Brother-Yu
 *
 */
public class Mining2CANS {
	public static boolean isRunning = true;
	/**
	 * 散列项集存放到对应的桶
	 */
	public static HashMap<Integer,List<String>> buckets = new HashMap<Integer,List<String>>();
	/**
	 * 原始事务集
	 */
	public static List<String> affairDataSet = new ArrayList<String>();
	/**
	 * 新事务集
	 */
	public static List<String> newAffairDataSet = new ArrayList<String>();
	/**
	 * hadoop根路径
	 */
	public static final String rootPath = "hdfs://192.168.178.131:9000//";
	public static int i = 1;
	public static List<String> fis1 = new ArrayList<String>();
	public static List<String> cans2 = new ArrayList<String>();
	public static int fis1Size = 0 ;
	public static class mapper extends Mapper<LongWritable, Text, Text, NullWritable> {  
		private  Text k = new Text();  	
		/**
		 * 读取原始事务集
		 */
		public void setup(Context context) {
			System.out.println("开始生成候选2项集...");
			i = 1;
			//事务集的路径
		
			String fis1Path = "hdfs://192.168.178.131:9000/Apriori/FIMk/fimk.txt";
			//读取1-频繁项集

			// HDFSFileUtils hdfs = new HDFSFileUtils();
			 fis1= HDFSFileUtils.readFile2List(fis1Path);
			//1-频繁项集的数目
			fis1Size = fis1.size();
		
		}
		public void map(LongWritable key, Text value, Context context)  {
			
		    String line = value.toString();  
		    int index = fis1.indexOf(line);
		    String newLine = "";
		    for(int j = index+1 ;j<fis1Size;j++) {
		    	int first  = Integer.parseInt(line);
		    	int second = Integer.parseInt(fis1.get(j));
		    	//排序，保证数据有序
		    	if(first <= second)
		    		newLine += first+" "+second;
		    	else
		    		newLine += second+" "+first;
		    	k.set(newLine);
		    	
		    	try {
		    		System.out.println("生成"+newLine);
					context.write(k, NullWritable.get());
				} catch (IOException | InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		    	newLine = "";
		    }
		}	
	}
	public static class reducer extends Reducer<Text, Text,Text, NullWritable> {  
		public void reduce(Text key, Iterable<Text> values, Context context){  
		    	try {
		    		cans2.add(key.toString());
					context.write(key,NullWritable.get());
				} catch (IOException | InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		}
	
		public void cleanup(Context context) {
		
			System.out.println("生成候选2项集结束...");
		}

	}
	public static Job getJob(String input,String output) throws IllegalArgumentException, IOException {
		if(HDFSFileUtils.checkFileExist(output)) {
			HDFSFileUtils.deleteFile(output);
		}
		Configuration conf = new Configuration();  
	    Job job = new Job(conf);  
	    job.setJarByClass(Mining2CANS.class);  
	    job.setJobName("Mining2CANS");  
  
	    job.setMapperClass(mapper.class);  
	    job.setReducerClass(reducer.class);  
	    //mapper输出的数据类型是什么？
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
	    FileInputFormat.addInputPath(job, new Path(input));  
	    FileOutputFormat.setOutputPath(job, new Path(output)); 
	    return job;
	}
	public static boolean run(String input,String output) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
		if(HDFSFileUtils.checkFileExist(output)) {
			HDFSFileUtils.deleteFile(output);
		}
		Configuration conf = new Configuration();  
	    Job job = new Job(conf);  
	    job.setJarByClass(Mining2CANS.class);  
	    job.setJobName("Mining2CANS");  
  
	    job.setMapperClass(mapper.class);  
	    job.setReducerClass(reducer.class);  
	    //mapper输出的数据类型是什么？
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
	    FileInputFormat.addInputPath(job, new Path(input));  
	    FileOutputFormat.setOutputPath(job, new Path(output));  
	    if(job.waitForCompletion(true)) {
	    	//产生新的事务集合
//	    	if(cans2.size()!=0) {
//				String affairDataPath = "hdfs://192.168.178.131:9000/Apriori/data-preprossing/data.txt";	
//
//			}
	  //  	Common.generateNewAffairData(newAffairDataSet);
	    	HDFSFileUtils.renameMV(output+"//part-r-00000", output+"//candidateSet.txt");
	    	return true;
	    }else {
	    	return false;
	    }
	}
}
