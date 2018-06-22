package Util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Array;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import Mining.Main;

public class Common {
	public static void main(String[] args){
	
//		System.out.println("done1");
//		for(int i=0;i<4000000;i++)
//		{
//			sb.append(i).append(" ");
//			
//		}
//		list = Arrays.asList(sb.toString().split(" "));
		System.out.println("done2");
//		HDFSFileUtils.readFile("hdfs://192.168.178.131:9000/Apriori/data-preprossing/data.txt");
//		 ArrayList<Integer> L = new ArrayList<Integer>();
//		    L.add(1);
//		    L.add(2);
//		    L.add(3);
//		    System.out.println(getSubset(L));
//		List<String> s = new ArrayList<String>();
//		s.add("1");
//		s.add("2");
//		s.add("5");
//		Set<String>set = new HashSet<String>(s);
//		set = subSet(set, 2);
//		for(String str:set) {
//			System.out.println(str);
//		}
//		System.out.println(checkCandidateItemIsFrequent(s,"1 2 5"));
		
//		System.out.println(generate2CandidateSet(s).toString());
//		Set<String> set = new HashSet<String>();
//		set.add("AA");
//		set.add("BB");
//		set.add("CC");
//		String str = StringUtils.join(set.toArray(), " ");
//      System.out.println("��������ת����"+str);
		String line = "6";
	//	System.out.println(line.split(" ")[0]);
		StringTokenizer token = new StringTokenizer(line);  
		while (token.hasMoreTokens()) { 
		    	String w = token.nextToken();
	//	    	System.out.println(w);
		}
//		List<String> fisk = new ArrayList<String>();
//		fisk.add("1 2 3 5");
//		fisk.add("1 2 3 6");
//		fisk.add("1 2 5 6");
//
//		List<String> list = filterCandidateSet(fisk);
//		
//		List<String> set2 = getNextCandidate(fisk);
//		
//		for(String s1:set2) {
//		//	System.out.println(s1);
//		}
//		
//		for(String str:list) {
//			System.out.println(str);
//		}
//		List<String> item = new ArrayList<String>();
//		item.add("1");
//		item.add("5");
//		System.out.println(getCandidateSetCount(fisk,item));
		List<String> fis = new ArrayList<String>();
		fis.add("1 2");
		fis.add("1 3");
		fis.add("1 5");
//		fis.add("2 3");
//		fis.add("2 4");
//		fis.add("2 5");
		List<String> candidateSet  = new ArrayList<String>();
		candidateSet.add("1");
		candidateSet.add("2");
		candidateSet.add("3");
		candidateSet.add("5");
		List<String> s1  = new ArrayList<String>();
		s1.add("100");
		s1.add("30");
		s1.add("2");
		s1.add("50");
		List<String> fis1  = new ArrayList<String>();
		List<String> can = new ArrayList<String>();
		can.add("2");
		can.add("4");
		can.add("5");
		fis1.add("1 2 4");
		fis1.add("1 2 5");
		System.out.println(getSupport(can,fis1));
//		candidateSet.add("1 2 5");
//		candidateSet.add("1 3 5");
//		candidateSet.add("2 3 4");
//		candidateSet.add("2 3 5");
//		candidateSet.add("2 4 5");
//		List<String> c  = checkCandidateSetIsFrequent(fis,candidateSet);
//		for(String str:c) {
//			System.out.println(str);
//		}
//		List<String> ret = generateAffairLineContainCandidateSet(candidateSet,fis);
//		System.out.println(ret.toString());
//		System.out.println(generateNextCandidateSet(s1,fis1));
		
	}
	/**
	 * �õ���ѡ��ļ�
	 * @param can
	 * @param affairSet
	 * @return
	 */
	public static int getSupport(List<String> can,List<String> affairSet) {
		int support = 0;
		for(String line:affairSet) {
			List<String> affairLine = Arrays.asList(StringUtils.split(line," "));
			if(affairLine.size()>=can.size()){
				if(affairLine.containsAll(can)) {
					support++;
				}
			}	
		}
		return support;
	}
	public static ArrayList<ArrayList<Integer>> getSubset(ArrayList<Integer> L) {
        if (L.size() > 0) {
            ArrayList<ArrayList<Integer>> result = new ArrayList<ArrayList<Integer>>();
            for (int i = 0; i < Math.pow(2, L.size()); i++) {// �����Ӽ�����=2�ĸü��ϳ��ȵĳ˷�
                ArrayList<Integer> subSet = new ArrayList<Integer>();
                int index = i;// ������0һֱ��2�ļ��ϳ��ȵĳ˷�-1
                for (int j = 0; j < L.size(); j++) {
                    // ͨ����һλ�ƣ��ж�������һλ��1������ǣ�����Ӵ���
                    if ((index & 1) == 1) {// λ�����㣬�ж����һλ�Ƿ�Ϊ1
                        subSet.add(L.get(j));
                    }
                    index >>= 1;// ��������һλ
                }
                result.add(subSet); // ���Ӽ��洢����
            }
            return result;
        } else {
            return null;
    }
}
	public static List<String> filterAffairSet(String path,List<String> cans2){
		List<String> newAffairSet = new ArrayList<String>();
		if(cans2.size()!=0) {
		//	int k = Common.string2List(cans2.get(0)).size();
			int k =cans2.get(0).split(" ").length;
			Configuration conf = new Configuration();   
	        FileSystem fs = null;
			try {
				fs = FileSystem.get(URI.create(path), conf);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			Path file = new Path(path);
			FSDataInputStream inStream = null;
			try {
				inStream = fs.open(file);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			BufferedReader bf=new BufferedReader(new InputStreamReader(inStream));//��ֹ��������
			String str = null;
			try {

				while ((str = bf.readLine()) != null) {	
			//		List<String> list = Common.string2List(str);
					List<String> list = new LinkedList<String>();
					list = Arrays.asList(StringUtils.split(str," "));
					boolean isContainItem = false;
					//ֻ�г��ȴ���k�������вű�������Ϊ�����ĺ�ѡ��ĵ�������Ϊk+1
					if(list.size()>k) {
						System.out.println("�ж�"+list+"����");
						//�Ƿ�kƵ���
						for(String line:cans2) {
							
						//	List<String> cans2LineList = new LinkedList<String>();
						//	cans2LineList = Arrays.asList(StringUtils.split(line," "));
							
							if(list.containsAll(Arrays.asList(StringUtils.split(line," ")))) {
								isContainItem = true;
								break;
							}	
						//	cans2LineList = null;
						}
						//�������kƵ�����������������
						if(isContainItem) {
						
							newAffairSet.add(str);
						}
					}
					list = null;
				}

			} catch (IOException e) {
			
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return newAffairSet;
	}
	/**
	 * ��ȡԭʼ����
	 * @param path ԭʼ���񼯵�·��
	 * @return
	 */
	public static List<String> readAffairSet(String path){
		List<String> affairSet = new ArrayList<String>();
		try {
			if(HDFSFileUtils.checkFileExist(path)) {
				String content = HDFSFileUtils.readFile(path);
				String [] arr = content.split("\r\n");
				affairSet = Arrays.asList(arr);
			}			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return affairSet;
	}
	/**
	 * �洢�µ���������
	 * @param data
	 */
	public static void generateNewAffairData(List<String> data) {
	//	StringBuffer sBuffer = list2StringBuffer(data);
		String output = "hdfs://192.168.178.131:9000/Apriori/data-preprossing/data.txt";
    	try {
			HDFSFileUtils.write2File(output,data);
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	/**
	 * ����2-��ѡ�
	 * @param source
	 * @return
	 */
	public static void generate2CandidateSet(String source, HashMap<Integer,List<String>> buckets){
	//	List<String> c2s = new ArrayList<String>();
		int i = 0;	
//		String [] arr = source.split(" ");
		List<String> list = new ArrayList<String>();
		StringTokenizer token = new StringTokenizer(source);  
		while (token.hasMoreTokens()) { 
		    	String w = token.nextToken();
		    	list.add(w);
		}
		int size= list.size();
	    System.out.println("ɨ�����񼯣��õ���ѡ2���");
		for(;i<size;i++) {
			  System.out.println("����"+(size-i));
			int j = i + 1;	
			for(;j<size;j++) {
				String c2 = list.get(i)+" "+list.get(j);
				//ͨ��ɢ�к������Ͱ��λ��
		    	int bucketIndex = Common.hashFunction(c2);
			    Set<Integer> bucketKeys = buckets.keySet();
			    if(bucketKeys.contains(bucketIndex)) {
			    	buckets.get(bucketIndex).add(c2);
			    }else {
			    	List<String> bucketContent = new ArrayList<String>();
			    	bucketContent.add(c2);
			    	buckets.put(bucketIndex, bucketContent);
			    }
			//	c2s.add(list.get(i)+" "+list.get(j));			
			}	
		}
		 System.out.println("ɨ�����񼯽���");
	//	return c2s;
	}
	/**
	 * ��ѡ2�����Ӧ��Ͱ��ַ
	 * @param x
	 * @param y
	 * @return
	 */
	public static int hashFunction(String line) {
		List<String> arr = new ArrayList<String>();
		StringTokenizer token = new StringTokenizer(line);  
		while (token.hasMoreTokens()) { 
		    	String w = token.nextToken();
		    	arr.add(w);
		}
		int x = Integer.valueOf(arr.get(0));
		int y = Integer.valueOf(arr.get(1));
		return (x*10+y)%7;
	}
	/**
	 * listתStringBuffer����Ҫ������Ϊ�˱���
	 * @param list
	 * @return
	 */
	public static StringBuffer list2StringBuffer(List<String> list) {
		StringBuffer sBuffer = new StringBuffer();
		for(String str:list) {
			sBuffer.append(str);
			sBuffer.append("\r\n");
		}
		return sBuffer;
	}
	/**
	 * ����һ���������������ĺ�ѡ���
	 * @param affairLine
	 * @param candidateSet
	 * @return
	 */
	public static List<String> generateAffairLineContainCandidateSet(List<String> affairLine,List<String> candidateSet){

		List<String> ret = new ArrayList<String>();
		for(String str:candidateSet) {
			List<String>  canLine = new LinkedList<String>();
			canLine = Arrays.asList(StringUtils.split(str," "));
			if(affairLine.containsAll(canLine)) {
				ret.add(str);
			}
			canLine = null;
		}
		return ret;
	}
	/**
	 * ��ѡ�����
	 * @param affairData ����
	 * @param candidateSet ��ѡ�
	 * @return
	 */
	public static int getCandidateSetCount(List<String> affairSet,List<String> candidateSet) {
		int count  = 0;

		for(String affair: affairSet) {
			List<String> affairLine = new ArrayList<String>();
			StringTokenizer token = new StringTokenizer(affair);  
		    while (token.hasMoreTokens()) { 
		    	String affairItem = token.nextToken();
		    	affairLine.add(affairItem);
		    }
		    //����������������1
		    if(affairLine.containsAll(candidateSet)) {
		    	count++;
		    } 	
		}	
		return count;
	}
	/**
	 * �����ض���ʽ���ַ���תlist
	 * @param line
	 * @return
	 */
//	public static List<String> string2List(String line){
//		List<String> list = new ArrayList<String>();
//	//	String [] arr = line.split(" ");
//		String str = new String(line);
//		list = Arrays.asList(str.split(" "));
//		
////		StringTokenizer token = new StringTokenizer(line);  
////		while (token.hasMoreTokens()) { 
////		    	String w = token.nextToken();
////		    	list.add(w);
////		}
//	// 	arr = null;
//		return list;
//	}
	/**
	 * �����ѡ2���HDFS
	 * @param buckets �����ѡ2��ĵĹ�ϣͰ
	 * @return
	 */
	public static boolean saveCAN2ToFile(HashMap<Integer,List<String>> buckets) {
		Set<Integer> bucketKeys = buckets.keySet();
    	Iterator<Integer> it = bucketKeys.iterator();
    	StringBuffer sBuffer = new StringBuffer();
    	//2Ƶ���
   // 	List<String> can2 = new ArrayList<String>();
    	List<String> c2List = new ArrayList<String>();;

    	while(it.hasNext()) {
    		int index = it.next();
        //  int count = buckets.get(index).size();

    //		can2.addAll(c2List);
    		for(String str:removeDuplicate(buckets.get(index))) {
    			c2List.add(str);
    		}
    	}
    	String output = "hdfs://192.168.178.131:9000/Apriori/CandidateSet/CandidateSet.txt";
    	try {
			return HDFSFileUtils.write2File(output,c2List);
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}
	/**
	 * �����ѡ�
	 * @param candidateSet ��ѡ�
	 * @return booleanֵ���Ƿ�洢�ɹ�
	 */
	public static boolean saveCandidateSet(List<String> candidateSet) {
		//StringBuffer sBuffer = list2StringBuffer(candidateSet);
		String output = "hdfs://192.168.178.131:9000/Apriori/CandidateSet/CandidateSet.txt";
    	try {
			return HDFSFileUtils.write2File(output,candidateSet);
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	return false;
	}
	/**
	 * ����ѡ��Ƿ���Ƶ���ģ���������֪ʶ
	 * k-1Ƶ���������ѡ�k��(k-1)�Ӽ���Ŀ�Ƿ�С��k
	 * @return
	 */
	public static List<String> checkCandidateSetIsFrequent(List<String> lastFis,List<String> candidateSet) {
		List<List<String>> lastFisList = new ArrayList<List<String>>();
		for(String line:lastFis) {
		//	List<String> lineList = string2List(line);
			List<String>  lineList= new LinkedList<String>();
			lineList = Arrays.asList(StringUtils.split(line," "));
			lastFisList.add(lineList);
			lineList = null;
		}
		List<String> frequentCandidateSet = new ArrayList<String>();
		for(String line:candidateSet) {
			int count = 0;
			//k-1�Ӽ�
			List<List<String>> subSet = new ArrayList<List<String>>();
		//	List<String> lineList = string2List(line);
			List<String>  lineList = new LinkedList<String>();
			 lineList = Arrays.asList(StringUtils.split(line," "));
	//		System.out.println(lineList);
			//���¡
            Set<String> lineTmp = new HashSet<String>(lineList);
			int k = lineList.size();
		
			//��ȡk-1���Ӽ�
			for(int i=0;i<k;i++) {
				lineList.remove(i);
				subSet.add(lineList);
				lineList = new ArrayList<String>(lineTmp);
			}
			for(List<String> subSetLine:subSet) {

				if(lastFisList.contains(subSetLine)) {
					count++;
				}
			}
			if(count == k) {
				frequentCandidateSet.add(line);
			}
			 lineList = null;
			
		}
		return frequentCandidateSet;
	}
	
	public static boolean checkCandidateItemIsFrequent(List<String> lastFis,String item) {

		boolean isFrequent = false;

		List<List<String>> lastFisList = new ArrayList<List<String>>();
		for(String line:lastFis) {
		//	List<String> lineList = string2List(line);
			
			List<String> lineList = new LinkedList<String>();
			lineList = Arrays.asList(StringUtils.split(line," "));
			lastFisList.add(lineList);
			lineList = null;
			
		}

		int count = 0;
		//k-1�Ӽ�
		List<List<String>> subSet = new ArrayList<List<String>>();
//		List<String> lineList = string2List(item);
		List<String> lineList = new LinkedList<String>();
		lineList = Arrays.asList(StringUtils.split(item," "));


        

		int k = lineList.size();

		//��ȡk-1���Ӽ�
		for(int i=0;i<k;i++) {
			List<String> tmp = new ArrayList<String>();


			for(int j = 0;j<k;j++) {
				if(j!=i) {
					tmp.add(lineList.get(j));
				}
			}
			subSet.add(tmp);

		}

		for(List<String> subSetLine:subSet) {
			if(lastFisList.contains(subSetLine)) {
				count++;
			}
		}

		if(count == k) {
			isFrequent = true;
		}
	
		return isFrequent;
	}
	/**
	 * ���˺�ѡ�<br>
	 * ���������ѭ�����̵ĵ�k���У�����k-1�����ɵ�k-1άƵ����Ŀ��������kά��ѡ��Ŀ����
	 * �����ڲ���k-1άƵ����Ŀ��ʱ�����ǿ���ʵ�ֶԸü��г���Ԫ�صĸ������м���������˶�ĳԪ�ض��ԣ�
	 * �����ļ�����������min_support�Ļ�����������ɾ����Ԫ�أ��Ӷ��ų��ɸ�Ԫ�ؽ�����Ĵ���������ϡ�
	 */
	public static List<String> filterCandidateSet(List<String> candidateSet){		
		List<String> filteredCandidateSet = new ArrayList<String>();
		Map<String,Integer> elementCount = new HashMap<String,Integer>();
		for(String line:candidateSet) {
			StringTokenizer token = new StringTokenizer(line);  
			while (token.hasMoreTokens()) { 
			    	String key = token.nextToken();
			    	int count = 1;
					if(elementCount.get(key) != null) {
						count = elementCount.get(key)+1;
					}
					elementCount.put(key, count);
			}
		}
		Set<String> keys = elementCount.keySet();
		Set<String> deletedElementSet = new HashSet<String>();
		for(String key:keys) {
			int count = elementCount.get(key);
			if(count<Main.min_support) {
				deletedElementSet.add(key);
			}
		}
		Iterator<String> it = candidateSet.iterator();
        while (it.hasNext()) {
            String line= it.next();
            boolean isContain = false;
			for(String key:deletedElementSet) {
				StringTokenizer token = new StringTokenizer(line);  
	        	while (token.hasMoreTokens()) { 
			    	String w = token.nextToken();
			    	if(key.equals(w)) {
			    		isContain = true;
			    		break;
			    	}
	        	}
			}
			if(!isContain)
				filteredCandidateSet.add(line);
        }
		return filteredCandidateSet;
	}
	/**
	 * Listȥ��
	 * @param ȥ�ع����List
	 * @return
	 */
	public static List<String> removeDuplicate(List<String> list) {  
		Set<String> c2Set = new HashSet<String>(list);
	    list.clear();   
	    list.addAll(c2Set);   
	    return list;   
	}
	public static List<String> lastItemConnect(List<String> items){
		List<String> connectResult = new ArrayList<String>();
		int i = 0;
		int size= items.size();
		
		for(;i<size;i++) {	
			int j = i + 1;	
			for(;j<size;j++) {
				String i1 = items.get(i);
				String i2 = items.get(j);
				if(Integer.parseInt(i1)>Integer.parseInt(i2)) {
					connectResult.add(i2+" "+i1);
				}	
				else {
					connectResult.add(i1+" "+i2);
				}		
			}	
		}
		return connectResult;
	}
	/**
	 * ����, �õ���һ�εĺ�ѡ�
	 * @param fisk  k-Ƶ���,ÿ�����������ִ�С����
	 * @return
	 */
	public static List<String> getNextCandidate(List<String> fisk){		
		 int size = fisk.size();
		 int i = 0,j;
		 List<String> CkSet = new ArrayList<String>();
		 for(;i<size;i++) {
			 String first[] = fisk.get(i).split(" ");
			 int len1 = first.length;
			 String last1 = first[len1-1];
			 //�洢����Ƶ��������ӽ��
			 for(j = i+1;j<size;j++) {
				 Set<String> Ck = new HashSet<String>();
				 String second[] = fisk.get(j).split(" ");
				 int len2 = second.length;
				 String last2 = second[len2-1];
				 String [] first_1 =  Arrays.copyOfRange(first, 0, len1-1);
				 String [] second_1 =  Arrays.copyOfRange(second, 0, len2-1);
				 //���ǰk-1����ͬ
				 if(StringUtils.join(first_1).equals(StringUtils.join(second_1))) {
					 //��k�ͬ
					 if(!last1.equals(last2)) {
						 CollectionUtils.addAll(Ck, first_1);
						 //�������ִ�С����
						 if(Integer.parseInt(last1)<Integer.parseInt(last2)) {
							 Ck.add(last1);
							 Ck.add(last2);
						 }else {
							 Ck.add(last2);
							 Ck.add(last1);
						 }
						 CkSet.add(array2String(Ck.toArray()));
					 } 
				 }
			 }
		 }
		
		 return CkSet;
	}
	
	public static List<String> generateNextCandidateSet(List<String> line,List<String> lastFis){
		 List<String> CkSet = new ArrayList<String>();
		int itemSize = line.size();
		String[] arr1 = new String[itemSize];
		arr1 = line.toArray(arr1);
		String [] first_1 =  Arrays.copyOfRange(arr1, 0, itemSize-1);
		String last1 = arr1[itemSize-1];
		int size = lastFis.size();
		int i = 0;
		for(;i<size;i++) {
			Set<String> Ck = new HashSet<String>();
			String second[] = lastFis.get(i).split(" ");
		
			String second_1[] = Arrays.copyOfRange(second, 0, itemSize-1);
			String last2 = second[itemSize-1];
			 //���ǰk-1����ͬ
			 if(StringUtils.join(first_1).equals(StringUtils.join(second_1))) {
				 //��k�ͬ
				 if(!last1.equals(last2)) {
					 CollectionUtils.addAll(Ck, first_1);
					 //�������ִ�С����
					 if(Integer.parseInt(last1)<Integer.parseInt(last2)) {
						 Ck.add(last1);
						 Ck.add(last2);
					 }else {
						 Ck.add(last2);
						 Ck.add(last1);
					 }
					 CkSet.add(array2String(Ck.toArray()));
				 }
			 }
		}
		 return CkSet;
	}
	/**
	 * listת�ַ���
	 * @param list
	 * @return
	 */
	public static String array2String(Object[] arr) {
		return StringUtils.join(arr, " ");
	}
	public static Set<String> subSet(Set<String> objSet, int size) {  
	    if (CollectionUtils.isEmpty(objSet)) {  
	        return Collections.emptySet();  
	    }  
	  
	    return ImmutableSet.copyOf(Iterables.limit(objSet, size));  
	}
}
