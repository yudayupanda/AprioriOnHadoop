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
 * �ھ�1Ƶ���������ɨ��1��ѡ�ʱ��ͨ��ɢ��Ͱ��ź�ѡ2�
 * @author Brother-Yu
 *
 */
public class Mining1FIS {
	public static boolean isRunning = true;
	/**
	 * ɢ�����ŵ���Ӧ��Ͱ
	 */
	public static HashMap<Integer,List<String>> buckets = new HashMap<Integer,List<String>>();
	/**
	 * ԭʼ����
	 */
	public static List<String> affairDataSet = new ArrayList<String>();
	/**
	 * ������
	 */
	public static List<String> newAffairDataSet = new ArrayList<String>();
	/**
	 * hadoop��·��
	 */
	public static final String rootPath = "hdfs://192.168.178.131:9000//";
	public static int i = 1;
	public static List<String> fis1 = new ArrayList<String>();
	public static class mapper extends Mapper<LongWritable, Text, Text, IntWritable> {  
		private final IntWritable one = new IntWritable(1);  
		private Text word = new Text();		
		/**
		 * ��ȡԭʼ����
		 */
//		public void setup(Context context) {
//			System.out.println("�ھ�1Ƶ���...");
//			i = 1;
//			InputSplit inputSplit = (InputSplit) context.getInputSplit();
//			//���񼯵�·��
//			String affairDataPath = ((FileSplit) inputSplit).getPath().toString();//getParent().getName();
//			System.out.println(affairDataPath);
//			affairDataSet = HDFSFileUtils.readFile2List(affairDataPath);
//		}
		public void map(LongWritable key, Text value, Context context)  {
			System.out.println("��ѡ1�:"+(i));
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

			System.out.println("��ѡ1�:"+(i++)+"����");
		}

	}
	public static class reducer extends Reducer<Text, IntWritable, Text, NullWritable> {  
		public void reduce(Text key, Iterable<IntWritable> values, Context context){  
		    int sum = 0;  
		    for (IntWritable val : values) {  
		        sum += val.get();  
		    }  
		    try {
		    	//�����Ŀ���ڻ������С֧�ֶȼ���
		    	if(sum >= Main.min_support) {
		    		System.out.println("1Ƶ�����"+key.toString()+" ֧�ֶ�"+sum);
		    		context.write(key, NullWritable.get());
		    		//��ӵ�1Ƶ���
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
			System.out.println("��ʼ��������...");
			for(String affairLine:affairDataSet) {
//				if(fis1.containsAll(Arrays.asList(affairLine.split(" ")))) {
//					newAffairDataSet.add(affairLine);
//				}
				if(Arrays.asList(affairLine.split(" ")).size()>1) {
					newAffairDataSet.add(affairLine);
				}
			}
			System.out.println("�������񼯽���...");
		}
		//ɾ���������κ�Ƶ��1�����������ѹ�����������κ�Ƶ��1�������϶�������������Ƶ�����
//		public void cleanup(Context context) {
//			for(String rawLine : affairDataSet) {
//				List<String> list =  Common.string2List(rawLine);
//				//�Ƿ�������е�1Ƶ���
//				boolean isContainItem = false;
//				for(String item:fis1) {
//					if(list.contains(item)) {
//						isContainItem = true;
//						break;
//					}
//				}
//				//�������1Ƶ���������
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
	    //mapper���������������ʲô��
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
	    //mapper���������������ʲô��
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job, new Path(input));  
	    FileOutputFormat.setOutputPath(job, new Path(output));  
	    if(job.waitForCompletion(true)) {
	    	//�����µ����񼯺�
	    	Common.generateNewAffairData(newAffairDataSet);
	 	HDFSFileUtils.renameMV(output+"//part-r-00000", output+"//fimk.txt");
	    
	    	return true;
	    }else {
	    	return false;
	    }
	}
}
