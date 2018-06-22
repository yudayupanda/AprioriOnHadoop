package Mining;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;

import Util.Common;
import Util.HDFSFileUtils;




public class Main {
	public static boolean isEnd = false;
	public static boolean isRunning = true;
	public static double support = 0.25;
	public static  int min_support = 20;
	public static void main(String[] args){
	
		//ԭʼ��������
	//	String input1  = "hdfs://192.168.178.131:9000/Apriori/data/test.txt";
		String input1  = "hdfs://192.168.178.131:9000/Apriori/data/data40MB.dat";
		String output1 = "hdfs://192.168.178.131:9000/Apriori/data-preprossing/";
		String tmp = "hdfs://192.168.178.131:9000/Apriori/tmp/";
		//Ԥ��������
		String input2  = "hdfs://192.168.178.131:9000/Apriori/data-preprossing/data.txt";
		String output2 = "hdfs://192.168.178.131:9000/Apriori/FIMk/";
		//��֦
		String input3 = "hdfs://192.168.178.131:9000//Apriori//CandidateSet//candidateSet.txt";
		String output3 = "hdfs://192.168.178.131:9000//Apriori//CandidateSetTmp//";
		//����Ƶ���
		String input4  = "hdfs://192.168.178.131:9000//Apriori/data-preprossing/data.txt";
	//	String input4  = input3;
		String output4 = "hdfs://192.168.178.131:9000//Apriori/FIMk/";
	
		//������ѡ�
		String input5 = "hdfs://192.168.178.131:9000//Apriori/FIMk/";
		String output5 = "hdfs://192.168.178.131:9000//Apriori//CandidateSet//";
		
		String fisPath = "hdfs://192.168.178.131:9000/Apriori/FIMk/fimk.txt";
		/*
		 * Job1:1.��ȡ���񼯺�
		 * 		2.����Ƶ�����
		 * Job2:1.������ѡ�
		 */
		min_support = 2;
		try {
			
			long startTime=System.currentTimeMillis();//��¼����ʱ��
			min_support = GetMinSupport.run(input1, tmp,support);
			System.out.println("��С֧�ֶȣ�"+min_support);
			//Ԥ�������ֵ�����
			System.out.println(Preprocessing.run(input1, output1));
			Mining1FIS.run(input2, output2);
			Mining2CANS.run(fisPath, output5);
			MiningkFIS.run(input4, output4);
			SelfConnection.run(input5, output5);

			while(true) {
				if(MiningkFIS.run(input4, output4)==0)
					break;
				if(SelfConnection.run(input5, output5)==0)
					break;
				if(Pruning.run(input3, output3)==0)
						break;
			}
			long endTime=System.currentTimeMillis();//��¼����ʱ��  
			float excTime=(float)(endTime-startTime)/1000;     
			System.out.println("ִ��ʱ�䣺"+excTime+"s");
		} catch (IllegalArgumentException | ClassNotFoundException | IOException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		/*
		try {
			//֧�ֶȼ���ͳ��
		//	min_support = GetMinSupport.run(input1, tmp,support);
			min_support = 45976;

			System.out.println("��С֧�ֶȣ�"+min_support);
			int isFirst = 1;
			//Ԥ�������ֵ�����
//			System.out.println(Preprocessing.run(input1, output1));
			long startTime=System.currentTimeMillis();//��¼����ʱ�� 
			
			Job job1 = Mining1FIS.getJob(input2, output2);
			if(job1.waitForCompletion(true)) {
				Job job2 = Mining2CANS.getJob(fisPath, output5);
				if(job2.waitForCompletion(true)){
					boolean isRunning = true;
					while(isRunning) {
						if(HDFSFileUtils.isFileEmpty(output5+"part-r-00000")) {
							System.out.println("��ѡ�Ϊ�գ�����");  
							isRunning = false;
							break;
						}	
						
						if(isFirst == 1) {
							System.out.println("��ʼִ��job3");
							Job job3 = MiningkFIS.getJob(input4, output4);
							if(job3.waitForCompletion(true)) {
								//��������
								Common.generateNewAffairData(MiningkFIS.newAffairDataSet);
								//���Ƶ���Ϊ��
								if(HDFSFileUtils.isFileEmpty(fisPath)) {
									System.out.println("Ƶ���Ϊ�գ�����");  
									isRunning = false;
									break;
								}
								Job job4 = SelfConnection2.getJob(input5, output5);
								if(job4.waitForCompletion(true)){
									//�����ѡ�Ϊ��
									if(HDFSFileUtils.isFileEmpty(output5+"//part-r-00000")) {
										System.out.println("��ѡ�Ϊ�գ�����");  
										isRunning = false;
									}
									isFirst = 0;
								}
							}else {
								System.out.println("ʧ��");
								System.exit(1);
							}
							
						}else {
							Job job5 = Pruning.getJob(input3, output3);
							if(job5.waitForCompletion(true)){
							       HDFSFileUtils.deleteFile(input3);
							       //����ΪkƵ���
							       HDFSFileUtils.renameMV(output3+"//part-r-00000", "hdfs://192.168.178.131:9000//Apriori//CandidateSet//"+"part-r-00000");							 
								  // System.exit(1);
							       Job job3 = MiningkFIS.getJob(input4, output4);
							       if(job3.waitForCompletion(true)) {
										//��������
										Common.generateNewAffairData(MiningkFIS.newAffairDataSet);
										//���Ƶ���Ϊ��
										if(HDFSFileUtils.isFileEmpty(fisPath)) {
											System.out.println("Ƶ���Ϊ�գ�����");  
											isRunning = false;
											break;
										}
										Job job4 = SelfConnection2.getJob(input5, output5);
										if(job4.waitForCompletion(true)){
											//�����ѡ�Ϊ��
											if(HDFSFileUtils.isFileEmpty(output5+"//part-r-00000")) {
												System.out.println("��ѡ�Ϊ�գ�����");  
												isRunning = false;
												break;
											}
										
										
										}
									}
							
							}
						}
						
					}
					 long endTime=System.currentTimeMillis();//��¼����ʱ��  
					 float excTime=(float)(endTime-startTime)/1000;     
					 System.out.println("ִ��ʱ�䣺"+excTime+"s");
	
				}
			}
			

		   
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		*/
	}
}
