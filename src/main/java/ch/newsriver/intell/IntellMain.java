package ch.newsriver.intell;

import ch.newsriver.executable.Main;
import org.apache.commons.cli.Options;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Created by eliapalme on 11/03/16.
 */
public class IntellMain extends Main {

    private static final int DEFAUTL_PORT = 9098;
    private static final Logger logger = LogManager.getLogger(IntellMain.class);


    public int getDefaultPort(){
        return DEFAUTL_PORT;
    }

    static Intell intell;

    public IntellMain(String[] args, Options options ){
        super(args,options,true);


    }

    public static void main(String[] args){

        Options options = new Options();

        options.addOption("f","pidfile", true, "pid file location");
        options.addOption(org.apache.commons.cli.Option.builder("p").longOpt("port").hasArg().type(Number.class).desc("port number").build());

        new IntellMain(args,options);

    }

    public void shutdown(){

        if(intell!=null)intell.stop();
    }

    public void start(){
        try {
            intell = new Intell();
            new Thread(intell).start();
        } catch (Exception e) {
            logger.fatal("Unable to initialize scout", e);
        }
    }


}
