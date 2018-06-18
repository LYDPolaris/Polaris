package lyd.ai.native4j.ck;

import java.io.File;
import java.util.Scanner;
import java.util.concurrent.*;

public class TPT {

    public static void main(String[] args) {
        String directory;
        String keyWord;

        if (args.length ==2){
            directory=args[0];
            keyWord=args[1];
        }else{
            Scanner in =new Scanner(System.in);
            directory=in.nextLine();
            keyWord=in.nextLine();
        }

        ExecutorService pool = Executors.newCachedThreadPool();
        MatchCounter counter= new MatchCounter(new File(directory),keyWord,pool);

        Future<Integer> result =pool.submit(counter);

        try {
            System.err.println(result.get()+" matching  files.");
        }catch (InterruptedException |ExecutionException e){
            e.getStackTrace();
        }

        pool.shutdown();

        int largrPoolSize =((ThreadPoolExecutor)pool).getLargestPoolSize();
        System.err.println("largest pool size   "+largrPoolSize);
    }
}
