package Mining;

import java.io.IOException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.commons.lang.StringUtils;
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

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


import Util.Common;
import Util.HDFSFileUtils;
/**
 * 挖掘1频繁项集，并在扫描1候选项集时，通过散列桶存放候选2项集
 * @author Brother-Yu
 *
 */
public class MiningkFIS {
	
	
	//public static int min_support = 2;
	/**
	 * 候选项集
	 */
	public static List<String> candidateSet;
	/**
	 * 原始事务集
	 */
	public static List<String> affairDataSet;
	/**
	 * 新事务集
	 */
	public static List<String> newAffairDataSet;
	/**
	 * 频繁项集
	 */
	public static List<String> fisk;
	public static int i = 1;
	/**
	 * k+1候选项集
	 */
	public static List<String> nextCandidate = new ArrayList<String>();
	public static int n;
	public static List<String> canset;

	public static class mapper extends Mapper<LongWritable, Text, Text, IntWritable> {  	
		private Text k = new Text();
		private IntWritable one = new IntWritable(1);  
		/**
		 * 读取原始事务集
		 */
		public void setup(Context context) {
			i = 1;
			System.out.println("开始挖掘频繁项集...");

			//初始化，每次迭代都需要初始化
			affairDataSet = new ArrayList<String>();
			newAffairDataSet = new ArrayList<String>();
			fisk = new ArrayList<String>();
			nextCandidate = new ArrayList<String>();
			candidateSet = new ArrayList<String>();
			canset = new ArrayList<String>();
			String candidateSetPath = "hdfs://192.168.178.131:9000//Apriori//CandidateSet//candidateSet.txt";
		//	String candidateSetPath = "hdfs://192.168.178.131:9000//Apriori//CandidateSet//part-r-00000";
			String affairPath = "hdfs://192.168.178.131:9000/Apriori/data-preprossing/data.txt";
			try {
				candidateSet =  HDFSFileUtils.readFile2List(candidateSetPath);
				//affairDataSet = HDFSFileUtils.readFile2List(affairPath);
				if(candidateSet.size()!=0) {
					n = candidateSet.get(0).split(" ").length;
				}
				System.out.println("初始化结束...");
			}catch(Exception e) {
				e.printStackTrace();
				System.exit(1);
			}
		}
		public void map(LongWritable key, Text value, Context context)  {
			String line = value.toString();
			List<String> affairLine =  new ArrayList<String>();
			StringTokenizer token = new StringTokenizer(line);  
		    while (token.hasMoreTokens()) { 
		    	String item = token.nextToken();
		    	affairLine.add(item);
		    }

		    affairDataSet.add(line);
		    //生成该行所拥有的候选项集
		    if(affairLine.size()>=n) {
				for(String str:candidateSet) {
				//	System.out.println("length"+candidateSet);
					List<String> list = Arrays.asList(StringUtils.split(str," "));
					if(affairLine.containsAll(list)) {
					//	System.out.println("开始输出第"+i+"条事务集的候选项集："+str);
						try {
							k.set(str);
							context.write(k, one);
						} catch (IOException | InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
					list = null;
				}
				
		    }
		    affairLine = null;
		    i++;
		}
	}
	public static class reducer extends Reducer<Text, IntWritable, Text, NullWritable> { 

		public void setup(Context context) {

		}
		public void reduce(Text key, Iterable<IntWritable> values, Context context){  
		    int sum = 0;  
		    for (IntWritable val : values) { 
		        sum += val.get();  
		    }
		    try {
		    	
		    	//如果数目大于或等于最小支持度计数
		    	if(sum >= Main.min_support) {
		    		System.out.println("频繁项目集:"+key.toString()+"支持度："+sum);
		    		context.write(key, NullWritable.get());
		    		//添加到k频繁项集
		    		fisk.add(key.toString());
		    	}else {
		    		System.out.println("不满足候选项目集:"+key.toString()+"支持度"+sum);
		    	}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}  
		}
		//删除不包含任何频繁k项集的事务：事务压缩
		public void cleanup(Context context) {
			
//			if(fisk.size() == 1 && fisk.size() ==0) {
//				isRunning = false;
//				
//			}
		//	int k = Common.string2List(fisk.get(0)).size();
			int k = fisk.get(0).split(" ").length;
			System.out.println("更新事务集...");
			//1.更新事务集
			for(String rawLine : affairDataSet) {
		//		List<String> list = Common.string2List(rawLine);
				List<String> list = new LinkedList<String>();
				list = Arrays.asList(StringUtils.split(rawLine," "));
				boolean isContainItem = false;
				
				//只有长度大于k的事务行才保留，因为产生的候选项集的单个长度为k+1
				if(list.size()>k) {
					//是否k频繁项集
					for(String fiskLineStr:fisk) {
					//	List<String> fiskLineList = Common.string2List(fiskLineStr);	
						List<String> fiskLineList = new LinkedList<String>();
						fiskLineList = Arrays.asList(StringUtils.split(fiskLineStr," "));
						if(list.containsAll(fiskLineList)) {
							isContainItem = true;
							break;
						}
						fiskLineList = null;
					}
					//如果包含k频繁项集，则保留该行事务
					if(isContainItem) {
						newAffairDataSet.add(rawLine);
					}
				}
				list = null;
			}
			System.out.println("更新事务集结束,新的事务集合长度："+newAffairDataSet.size());
		
		}
		
	} 
	public static Job getJob(String input,String output){
		try {
			if(HDFSFileUtils.checkFileExist(output)) {
				HDFSFileUtils.deleteFile(output);
			}
		} catch (IllegalArgumentException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		Configuration conf = new Configuration();  
	    Job job = null;
		try {
			job = new Job(conf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}  
	    job.setJarByClass(MiningkFIS.class);  
	    job.setJobName("miningkFIS");  
  
	    job.setMapperClass(mapper.class);  
	//    job.setCombinerClass(reducer.class);
	    job.setReducerClass(reducer.class);  
	    //mapper输出的数据类型是什么？
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
	    try {
			FileInputFormat.addInputPath(job, new Path(input));
		} catch (IllegalArgumentException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}  
	    FileOutputFormat.setOutputPath(job, new Path(output)); 
	    return job;
	}
	
	public static int run(String input,String output) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
		if(HDFSFileUtils.checkFileExist(output)) {
			HDFSFileUtils.deleteFile(output);
		}
		Configuration conf = new Configuration();  
	    Job job = new Job(conf);  
	    job.setJarByClass(MiningkFIS.class);  
	    job.setJobName("miningkFIS");  
  
	    job.setMapperClass(mapper.class);  
	//    job.setCombinerClass(reducer.class);
	    job.setReducerClass(reducer.class);  
	    //mapper输出的数据类型是什么？
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job, new Path(input));  
	    FileOutputFormat.setOutputPath(job, new Path(output));  
	    if(job.waitForCompletion(true)) {
	    	Common.generateNewAffairData(newAffairDataSet);
	    	if(HDFSFileUtils.isFileEmpty(output+"//part-r-00000"))
	    		return 0;
	    	else {
	    		//保存为k频繁项集
	    		HDFSFileUtils.renameMV(output+"//part-r-00000", output+"//FIMk.txt");
		    	return 1;
	    	}
	    	
	    }else {
	    	return -1;
	    } 
	}
}
