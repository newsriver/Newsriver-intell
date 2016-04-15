package ch.newsriver.intell;

import ch.newsriver.executable.Main;
import ch.newsriver.executable.poolExecution.MainWithPoolExecutorOptions;
import org.apache.commons.cli.Options;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Created by eliapalme on 11/03/16.
 */
public class IntellMain extends MainWithPoolExecutorOptions {

    private static final int DEFAUTL_PORT = 9095;
    private static final Logger logger = LogManager.getLogger(IntellMain.class);


    public int getDefaultPort(){
        return DEFAUTL_PORT;
    }

    static Intell intell;

    public IntellMain(String[] args){
        super(args,true);
    }

    public static void main(String[] args){
        new IntellMain(args);
    }

    public void shutdown(){

        if(intell!=null)intell.stop();
    }

    public void start(){
        try {
            System.out.println("Threads pool size:" + this.getPoolSize() +"\tbatch size:"+this.getBatchSize()+"\tqueue size:"+this.getBatchSize());
            intell = new Intell(this.getPoolSize(),this.getBatchSize(),this.getQueueSize());
            new Thread(intell).start();
        } catch (Exception e) {
            logger.fatal("Unable to initialize scout", e);
        }
    }


}
