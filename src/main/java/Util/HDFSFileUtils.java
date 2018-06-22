package Util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class HDFSFileUtils {
	private FileSystem hdfs;
	public static boolean isFileEmpty(String path) {
		if(readFile2List(path).size() == 0)
			return true;
		return false;
	}
	/**
	 * 读文件
	 * @param filePath
	 * @return
	 * @throws IOException
	 */
	public   static String readFile(String filePath){
		Configuration conf = new Configuration();   
        FileSystem fs = null;
		try {
			fs = FileSystem.get(URI.create(filePath), conf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		Path file = new Path(filePath);
		FSDataInputStream inStream = null;
		try {
			inStream = fs.open(file);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		BufferedReader bf=new BufferedReader(new InputStreamReader(inStream));//防止中文乱码
		String line = null;
		StringBuffer sBuffer = new StringBuffer();
		try {

			while ((line = bf.readLine()) != null) {
					sBuffer.append(line);
					sBuffer.append("\r\n");
			}

		} catch (IOException e) {
			System.out.println("出错");
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return sBuffer.toString();
	}
	
	public  static List<String> readFile2List(String filePath) {
		List<String> list = new ArrayList<String>();
		Configuration conf = new Configuration();   
        FileSystem fs = null;
		try {
			fs = FileSystem.get(URI.create(filePath), conf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		Path file = new Path(filePath);
		FSDataInputStream inStream = null;
		try {
			inStream = fs.open(file);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(1);
		}
		BufferedReader bf=new BufferedReader(new InputStreamReader(inStream));//防止中文乱码
		String line = null;
	
		try {

			while ((line = bf.readLine()) != null) {
					if(line.length()!=0) {
						list.add(line);
					}
			}

		} catch (IOException e) {
			System.out.println("出错");
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return list;
	}
	/**
	 * 统计文件行数
	 * @param filePath
	 * @return
	 * @throws IOException
	 */
	public static int countFileLine(String filePath) throws IOException {
		System.out.println("start...");
		Configuration conf = new Configuration();   
        FileSystem fs = FileSystem.get(URI.create(filePath), conf);
        int count = 0;
		Path file = new Path(filePath);
		FSDataInputStream inStream = fs.open(file);
		BufferedReader bf=new BufferedReader(new InputStreamReader(inStream));//防止中文乱码
		while ((bf.readLine()) != null) {
			System.out.println(count);
			count++;
		}
	
		return count;
	}
	/**
	 * 写文件
	 * @param filePath 文件路径
	 * @param words	文件内容
	 * @return 布尔值：是否写入成功
	 * @throws UnsupportedEncodingException
	 * @throws IOException
	 */
	public static boolean write2File(String filePath,List<String> lines) throws UnsupportedEncodingException, IOException {
		boolean status = true;
		FSDataOutputStream out = createFile(filePath);
		if( null == out) {
			status = false;
		}		
		else {
			//out.writeBytes(words);
			for(String line:lines) {
				line = line.trim();
				if(line.length()!=0) {
					out.write(line.getBytes("GBK"));
					out.write("\r\n".getBytes("GBK"));  
				}
			}
			
			out.close();  
		}
		return status;
	}

	/**
	 * 创建文件
	 * @param file 文件路径
	 * @return FSDataOutputStream out,若创建失败，则返回null
	 */
	public static FSDataOutputStream createFile(String filePath) {
		Configuration conf = new Configuration();  
		FSDataOutputStream out = null;
		FileSystem fs = null;
		try {
			//如果文件存在则删除
			if(checkFileExist(filePath)) {
				deleteFile(filePath);
			}	
			fs = FileSystem.get(URI.create(filePath), conf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			
		}  

		Path path = new Path(filePath);  

		try {
			out = fs.create(path);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		
		}   //创建文件  
		return out;
	}
	/**
	 * 判断文件是否存在
	 * @param pathStr
	 * @return
	 * @throws IOException
	 */
	public static boolean checkFileExist(String pathStr) throws IOException {		
		Path path = new Path(pathStr);
		return path.getFileSystem(new Configuration()).exists(path);
	}
	/**
	 * 删除文件
	 */
	public static boolean deleteFile(String dest) throws IllegalArgumentException, IOException {
        // TODO Auto-generated method stub  
		Path path = new Path(dest);
        return path.getFileSystem(new Configuration()).delete(new Path(dest), true);
    }
	/**
	 * 上传文件
	 * @param src 源地址
	 * @param dest 目的地址
	 * @throws IOException
	 */
    public void upload(String src, String dest) throws IOException {
        // TODO Auto-generated method stub
        FileInputStream in = new FileInputStream(src);
    	Path path = new Path(src);
        FSDataOutputStream os = path.getFileSystem(new Configuration()).create(new Path(dest), true);
        IOUtils.copyBytes(in, os, 4096, true);
    }
    /**
     * 创建文件夹
     * @param dest
     * @return
     * @throws IllegalArgumentException
     * @throws IOException
     */
    public boolean makeDir(String dest) throws IllegalArgumentException, IOException {
    	Path path = new Path(dest);
        return path.getFileSystem(new Configuration()).mkdirs(new Path(dest));
    }
  //重命名和移动  
    public static void renameMV(String src,String dst) throws IOException{  
    	Path path = new Path(src);
        FileSystem fs = path.getFileSystem(new Configuration());  
        fs.rename(new Path(src), new Path(dst));  
        fs.close();  
    } 
 // 下载显示进度
    public void download2(String dest, Map<String, Integer> descript) throws IllegalArgumentException, IOException {
        FSDataInputStream in = this.hdfs.open(new Path(dest));
        descript.put("byteSize", in.available());
        descript.put("current", 0);
        byte[] bs = new byte[1024];
        while (-1 != (in.read(bs))) {
            descript.put("current", descript.get("current") + 1024);
        }
        in.close();
    }

    // 上传显示进度
    public void upload2(String src, String dest, Map<String, Long> descript)
            throws IllegalArgumentException, IOException {
        File file = new File(src);
        FileInputStream in = new FileInputStream(file);
        FSDataOutputStream out = this.hdfs.create(new Path(dest), true);
        descript.put("byteSize", file.length());
        descript.put("current", 0l);
        // 0.5mb
        byte[] bs = new byte[1024 * 1024 / 2];
        while (-1 != (in.read(bs))) {
            out.write(bs);
            descript.put("current", descript.get("current") + 1024);
        }
        out.close();
        in.close();
    }
}
