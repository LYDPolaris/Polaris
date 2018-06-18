package lyd.ai.http.ck;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.io.monitor.FileAlterationListenerAdaptor;
import org.apache.commons.io.monitor.FileAlterationObserver;
import org.apache.log4j.Logger;  
  
/** 
 * Created by Rick on 2017/12/18. 
 * 
 * 文件变化监听器 
 * 
 * 在Apache的Commons-IO中有关于文件的监控功能的代码. 文件监控的原理如下： 
 * 由文件监控类FileAlterationMonitor中的线程不停的扫描文件观察器FileAlterationObserver， 
 * 如果有文件的变化，则根据相关的文件比较器，判断文件时新增，还是删除，还是更改。（默认为1000毫秒执行一次扫描） 
 * 
 * 
 */  
public class FilesListener extends FileAlterationListenerAdaptor {  
    private Logger log = Logger.getLogger(FilesListener.class);
    private static  SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    
    /** 
     * 文件创建执行 
     */  
    public void onFileCreate(File file) {  
        System.err.println(df.format(new Date()) + "  [新建]:" + file.getAbsolutePath());
        try{
        	 PerCombine.generateRowsList(file.getAbsolutePath());
        }	
        catch (IOException e){
          e.printStackTrace();
        }
        file.delete();
        
    }  
  
    /** 
     * 文件创建修改 
     */  
    public void onFileChange(File file) {  
    	System.err.println(df.format(new Date()) + "  [修改]:" + file.getAbsolutePath());
    }  
  
    /** 
     * 文件删除 
     */  
    public void onFileDelete(File file) {  
    	System.err.println(df.format(new Date()) + "  [删除]:" + file.getAbsolutePath());
    }  
  
    /** 
     * 目录创建 
     */  
    public void onDirectoryCreate(File directory) {  
        log.info("[新建]:" + directory.getAbsolutePath());  
    }  
  
    /** 
     * 目录修改 
     */  
    public void onDirectoryChange(File directory) {  
        log.info("[修改]:" + directory.getAbsolutePath());  
    }  
  
    /** 
     * 目录删除 
     */  
    public void onDirectoryDelete(File directory) {  
        log.info("[删除]:" + directory.getAbsolutePath());  
    }  
  
    public void onStart(FileAlterationObserver observer) {  
        // TODO Auto-generated method stub  
        super.onStart(observer);  
    }  
  
    public void onStop(FileAlterationObserver observer) {  
        // TODO Auto-generated method stub  
        super.onStop(observer);  
    }  
}  