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
//      System.out.println("特殊数组转换："+str);
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
	 * 得到候选项集的计
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
            for (int i = 0; i < Math.pow(2, L.size()); i++) {// 集合子集个数=2的该集合长度的乘方
                ArrayList<Integer> subSet = new ArrayList<Integer>();
                int index = i;// 索引从0一直到2的集合长度的乘方-1
                for (int j = 0; j < L.size(); j++) {
                    // 通过逐一位移，判断索引那一位是1，如果是，再添加此项
                    if ((index & 1) == 1) {// 位与运算，判断最后一位是否为1
                        subSet.add(L.get(j));
                    }
                    index >>= 1;// 索引右移一位
                }
                result.add(subSet); // 把子集存储起来
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
			BufferedReader bf=new BufferedReader(new InputStreamReader(inStream));//防止中文乱码
			String str = null;
			try {

				while ((str = bf.readLine()) != null) {	
			//		List<String> list = Common.string2List(str);
					List<String> list = new LinkedList<String>();
					list = Arrays.asList(StringUtils.split(str," "));
					boolean isContainItem = false;
					//只有长度大于k的事务行才保留，因为产生的候选项集的单个长度为k+1
					if(list.size()>k) {
						System.out.println("判断"+list+"过滤");
						//是否k频繁项集
						for(String line:cans2) {
							
						//	List<String> cans2LineList = new LinkedList<String>();
						//	cans2LineList = Arrays.asList(StringUtils.split(line," "));
							
							if(list.containsAll(Arrays.asList(StringUtils.split(line," ")))) {
								isContainItem = true;
								break;
							}	
						//	cans2LineList = null;
						}
						//如果包含k频繁项集，则保留该行事务
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
	 * 读取原始事务集
	 * @param path 原始事务集的路径
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
	 * 存储新的事务数据
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
	 * 产生2-候选项集
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
	    System.out.println("扫描事务集，得到候选2项集合");
		for(;i<size;i++) {
			  System.out.println("还差"+(size-i));
			int j = i + 1;	
			for(;j<size;j++) {
				String c2 = list.get(i)+" "+list.get(j);
				//通过散列函数存放桶的位置
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
		 System.out.println("扫描事务集结束");
	//	return c2s;
	}
	/**
	 * 候选2项集所对应的桶地址
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
	 * list转StringBuffer，主要作用是为了保存
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
	 * 产生一行事务项所包含的候选项集合
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
	 * 候选项集计数
	 * @param affairData 事务集
	 * @param candidateSet 候选项集
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
		    //如果包含，则计数加1
		    if(affairLine.containsAll(candidateSet)) {
		    	count++;
		    } 	
		}	
		return count;
	}
	/**
	 * 带有特定格式的字符串转list
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
	 * 保存候选2项集到HDFS
	 * @param buckets 保存候选2项集的的哈希桶
	 * @return
	 */
	public static boolean saveCAN2ToFile(HashMap<Integer,List<String>> buckets) {
		Set<Integer> bucketKeys = buckets.keySet();
    	Iterator<Integer> it = bucketKeys.iterator();
    	StringBuffer sBuffer = new StringBuffer();
    	//2频繁项集
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
	 * 保存候选项集
	 * @param candidateSet 候选项集
	 * @return boolean值：是否存储成功
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
	 * 检查候选项集是否是频繁的：基于先验知识
	 * k-1频繁项集包含候选项集k的(k-1)子集数目是否小于k
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
			//k-1子集
			List<List<String>> subSet = new ArrayList<List<String>>();
		//	List<String> lineList = string2List(line);
			List<String>  lineList = new LinkedList<String>();
			 lineList = Arrays.asList(StringUtils.split(line," "));
	//		System.out.println(lineList);
			//深克隆
            Set<String> lineTmp = new HashSet<String>(lineList);
			int k = lineList.size();
		
			//获取k-1项子集
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
		//k-1子集
		List<List<String>> subSet = new ArrayList<List<String>>();
//		List<String> lineList = string2List(item);
		List<String> lineList = new LinkedList<String>();
		lineList = Arrays.asList(StringUtils.split(item," "));


        

		int k = lineList.size();

		//获取k-1项子集
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
	 * 过滤候选项集<br>
	 * 在逐层搜索循环过程的第k步中，根据k-1步生成的k-1维频繁项目集来产生k维候选项目集，
	 * 由于在产生k-1维频繁项目集时，我们可以实现对该集中出现元素的个数进行计数处理，因此对某元素而言，
	 * 若它的计数个数不到min_support的话，可以事先删除该元素，从而排除由该元素将引起的大规格所有组合。
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
	 * List去重
	 * @param 去重过后的List
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
	 * 连接, 得到下一次的候选项集
	 * @param fisk  k-频繁项集,每个事务按照数字大小排序
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
			 //存储两个频繁项集的连接结果
			 for(j = i+1;j<size;j++) {
				 Set<String> Ck = new HashSet<String>();
				 String second[] = fisk.get(j).split(" ");
				 int len2 = second.length;
				 String last2 = second[len2-1];
				 String [] first_1 =  Arrays.copyOfRange(first, 0, len1-1);
				 String [] second_1 =  Arrays.copyOfRange(second, 0, len2-1);
				 //如果前k-1项相同
				 if(StringUtils.join(first_1).equals(StringUtils.join(second_1))) {
					 //第k项不同
					 if(!last1.equals(last2)) {
						 CollectionUtils.addAll(Ck, first_1);
						 //按照数字大小排序
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
			 //如果前k-1项相同
			 if(StringUtils.join(first_1).equals(StringUtils.join(second_1))) {
				 //第k项不同
				 if(!last1.equals(last2)) {
					 CollectionUtils.addAll(Ck, first_1);
					 //按照数字大小排序
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
	 * list转字符串
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
