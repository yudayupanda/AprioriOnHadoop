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
 * ������:������ѡ�
 * @author Brother-Yu
 *
 */
public class SelfConnection {

	public static int i = 1;
	public static class mapper extends Mapper<LongWritable, Text, Text, Text> {
		private Text k = new Text();
		private Text v = new Text();
		public void setup(Context context) {
			System.out.println("��ʼ������ѡ�...");
		
		}
		public void map(LongWritable key, Text value, Context context)  {
		//	System.out.println("��"+(i)+"��");
			String line = value.toString();
			//һ��Ƶ���
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
//	    		System.out.println("ǰ�벿�֣�"+keyString);
//	    		System.out.println("��벿�֣�"+lastItem);
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
					System.out.println("��ѡ�:"+first+" "+last);
					context.write(k, NullWritable.get());
				}
				
			} catch (IOException | InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		public void cleanup(Context context) {
				System.out.println("������ѡ�����...");
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
	    //mapper���������������ʲô��
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
	    //mapper���������������ʲô��
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job, new Path(input));  
	    FileOutputFormat.setOutputPath(job, new Path(output));  
	    if(job.waitForCompletion(true)) {
	    
	    	if(HDFSFileUtils.isFileEmpty(output+"//part-r-00000")) {
	    		System.out.println("��֦���ں�ѡ������н���������");
	    		return 0;
	    	}
	    	HDFSFileUtils.renameMV(output+"//part-r-00000", output+"//candidateSet.txt");
	    	return 1;
//	    	else {
//	    		//����Ϊk��ѡ�
//		    	HDFSFileUtils.renameMV(output+"//part-r-00000", output+"//CandidateSet.txt");
//		    	return 1;
//	    	}
	    }else {
	    	return -1;
	    }
		
	}
}
