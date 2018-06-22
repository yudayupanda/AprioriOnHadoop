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
public class Mining1FIS {
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
	public static class mapper extends Mapper<LongWritable, Text, Text, IntWritable> {  
		private final IntWritable one = new IntWritable(1);  
		private Text word = new Text();		
		/**
		 * 读取原始事务集
		 */
//		public void setup(Context context) {
//			System.out.println("挖掘1频繁项集...");
//			i = 1;
//			InputSplit inputSplit = (InputSplit) context.getInputSplit();
//			//事务集的路径
//			String affairDataPath = ((FileSplit) inputSplit).getPath().toString();//getParent().getName();
//			System.out.println(affairDataPath);
//			affairDataSet = HDFSFileUtils.readFile2List(affairDataPath);
//		}
		public void map(LongWritable key, Text value, Context context)  {
			System.out.println("候选1项集:"+(i));
			StringBuffer lineItem = new StringBuffer();
		    String line = value.toString(); 
		    affairDataSet.add(line);
		    StringTokenizer token = new StringTokenizer(line);  
		    while (token.hasMoreTokens()) { 
		    	String w = token.nextToken();
		        word.set(w);
		        lineItem.append(w+" ");
		        try {
		       
					context.write(word, one);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}  
		    }

			System.out.println("候选1项集:"+(i++)+"结束");
		}

	}
	public static class reducer extends Reducer<Text, IntWritable, Text, NullWritable> {  
		public void reduce(Text key, Iterable<IntWritable> values, Context context){  
		    int sum = 0;  
		    for (IntWritable val : values) {  
		        sum += val.get();  
		    }  
		    try {
		    	//如果数目大于或等于最小支持度计数
		    	if(sum >= Main.min_support) {
		    		System.out.println("1频繁项集："+key.toString()+" 支持度"+sum);
		    		context.write(key, NullWritable.get());
		    		//添加到1频繁项集
		    		fis1.add(key.toString());
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
			System.out.println("开始更新事务集...");
			for(String affairLine:affairDataSet) {
//				if(fis1.containsAll(Arrays.asList(affairLine.split(" ")))) {
//					newAffairDataSet.add(affairLine);
//				}
				if(Arrays.asList(affairLine.split(" ")).size()>1) {
					newAffairDataSet.add(affairLine);
				}
			}
			System.out.println("更新事务集结束...");
		}
		//删除不包含任何频繁1项集的事务：事务压缩（不包含任何频繁1项集的事物肯定不包含其他的频繁项集）
//		public void cleanup(Context context) {
//			for(String rawLine : affairDataSet) {
//				List<String> list =  Common.string2List(rawLine);
//				//是否包含所有的1频繁项集
//				boolean isContainItem = false;
//				for(String item:fis1) {
//					if(list.contains(item)) {
//						isContainItem = true;
//						break;
//					}
//				}
//				//如果包含1频繁项集，则保留
//				if(isContainItem) {
//					newAffairDataSet.add(rawLine);
//				}
//			}
//		}

	}
	public static Job getJob(String input,String output) throws IllegalArgumentException, IOException {
		if(HDFSFileUtils.checkFileExist(output)) {
			HDFSFileUtils.deleteFile(output);
		}
		Configuration conf = new Configuration();  
	    Job job = new Job(conf);  
	    job.setJarByClass(Mining1FIS.class);  
	    job.setJobName("Mining1FIS");  
  
	    job.setMapperClass(mapper.class);  
	    job.setReducerClass(reducer.class);  
	    //mapper输出的数据类型是什么？
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
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
	    job.setJarByClass(Mining1FIS.class);  
	    job.setJobName("Mining1FIS");  
  
	    job.setMapperClass(mapper.class);  
	    job.setReducerClass(reducer.class);  
	    //mapper输出的数据类型是什么？
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job, new Path(input));  
	    FileOutputFormat.setOutputPath(job, new Path(output));  
	    if(job.waitForCompletion(true)) {
	    	//产生新的事务集合
	    	Common.generateNewAffairData(newAffairDataSet);
	 	HDFSFileUtils.renameMV(output+"//part-r-00000", output+"//fimk.txt");
	    
	    	return true;
	    }else {
	    	return false;
	    }
	}
}
