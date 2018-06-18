package lyd.ai.http.ck;

import org.apache.commons.io.filefilter.FileFilterUtils;  
import org.apache.commons.io.filefilter.HiddenFileFilter;  
import org.apache.commons.io.filefilter.IOFileFilter;  
import org.apache.commons.io.monitor.FileAlterationMonitor;
import org.apache.commons.io.monitor.FileAlterationObserver;  
import org.apache.log4j.Logger;  
  
import java.io.*;
import java.net.InetAddress;
import java.util.concurrent.TimeUnit;  
  
public class MonitorF2CK  
{  
    final  static Logger logger = Logger.getLogger(MonitorF2CK.class);  
    public static void main(String[] args) throws Exception {  
        // 监控目录  
        String rootDir = "/bigdata/WGData/";  
        // 轮询间隔 5 秒  
        long interval = TimeUnit.SECONDS.toMillis(1);  
        // 创建过滤器  
        IOFileFilter directories = FileFilterUtils.and(  
                FileFilterUtils.directoryFileFilter(),  
                HiddenFileFilter.VISIBLE);  
        IOFileFilter files       = FileFilterUtils.and(  
                FileFilterUtils.fileFileFilter(),  
                FileFilterUtils.suffixFileFilter(".csv"));  
        IOFileFilter filter = FileFilterUtils.or(directories, files);  
        // 使用过滤器  
        FileAlterationObserver observer = new FileAlterationObserver(new File(rootDir), filter);  
        observer.addListener(new FilesListener());  
        //创建文件变化监听器  
        FileAlterationMonitor monitor = new FileAlterationMonitor(interval, observer);  
        // 开始监控  
        try{  
            monitor.start();  
            System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>监控中<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");  
        }  
        catch (Exception e){  
            logger.error("异常处理",e);  
        }  
    }  
}  