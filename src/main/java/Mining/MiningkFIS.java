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
 * �ھ�1Ƶ���������ɨ��1��ѡ�ʱ��ͨ��ɢ��Ͱ��ź�ѡ2�
 * @author Brother-Yu
 *
 */
public class MiningkFIS {
	
	
	//public static int min_support = 2;
	/**
	 * ��ѡ�
	 */
	public static List<String> candidateSet;
	/**
	 * ԭʼ����
	 */
	public static List<String> affairDataSet;
	/**
	 * ������
	 */
	public static List<String> newAffairDataSet;
	/**
	 * Ƶ���
	 */
	public static List<String> fisk;
	public static int i = 1;
	/**
	 * k+1��ѡ�
	 */
	public static List<String> nextCandidate = new ArrayList<String>();
	public static int n;
	public static List<String> canset;

	public static class mapper extends Mapper<LongWritable, Text, Text, IntWritable> {  	
		private Text k = new Text();
		private IntWritable one = new IntWritable(1);  
		/**
		 * ��ȡԭʼ����
		 */
		public void setup(Context context) {
			i = 1;
			System.out.println("��ʼ�ھ�Ƶ���...");

			//��ʼ����ÿ�ε�������Ҫ��ʼ��
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
				System.out.println("��ʼ������...");
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
		    //���ɸ�����ӵ�еĺ�ѡ�
		    if(affairLine.size()>=n) {
				for(String str:candidateSet) {
				//	System.out.println("length"+candidateSet);
					List<String> list = Arrays.asList(StringUtils.split(str," "));
					if(affairLine.containsAll(list)) {
					//	System.out.println("��ʼ�����"+i+"�����񼯵ĺ�ѡ���"+str);
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
		    	
		    	//�����Ŀ���ڻ������С֧�ֶȼ���
		    	if(sum >= Main.min_support) {
		    		System.out.println("Ƶ����Ŀ��:"+key.toString()+"֧�ֶȣ�"+sum);
		    		context.write(key, NullWritable.get());
		    		//��ӵ�kƵ���
		    		fisk.add(key.toString());
		    	}else {
		    		System.out.println("�������ѡ��Ŀ��:"+key.toString()+"֧�ֶ�"+sum);
		    	}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}  
		}
		//ɾ���������κ�Ƶ��k�����������ѹ��
		public void cleanup(Context context) {
			
//			if(fisk.size() == 1 && fisk.size() ==0) {
//				isRunning = false;
//				
//			}
		//	int k = Common.string2List(fisk.get(0)).size();
			int k = fisk.get(0).split(" ").length;
			System.out.println("��������...");
			//1.��������
			for(String rawLine : affairDataSet) {
		//		List<String> list = Common.string2List(rawLine);
				List<String> list = new LinkedList<String>();
				list = Arrays.asList(StringUtils.split(rawLine," "));
				boolean isContainItem = false;
				
				//ֻ�г��ȴ���k�������вű�������Ϊ�����ĺ�ѡ��ĵ�������Ϊk+1
				if(list.size()>k) {
					//�Ƿ�kƵ���
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
					//�������kƵ�����������������
					if(isContainItem) {
						newAffairDataSet.add(rawLine);
					}
				}
				list = null;
			}
			System.out.println("�������񼯽���,�µ����񼯺ϳ��ȣ�"+newAffairDataSet.size());
		
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
	    //mapper���������������ʲô��
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
	    //mapper���������������ʲô��
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job, new Path(input));  
	    FileOutputFormat.setOutputPath(job, new Path(output));  
	    if(job.waitForCompletion(true)) {
	    	Common.generateNewAffairData(newAffairDataSet);
	    	if(HDFSFileUtils.isFileEmpty(output+"//part-r-00000"))
	    		return 0;
	    	else {
	    		//����ΪkƵ���
	    		HDFSFileUtils.renameMV(output+"//part-r-00000", output+"//FIMk.txt");
		    	return 1;
	    	}
	    	
	    }else {
	    	return -1;
	    } 
	}
}
