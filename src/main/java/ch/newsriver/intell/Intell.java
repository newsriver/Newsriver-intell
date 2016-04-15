package ch.newsriver.intell;

import ch.newsriver.dao.ElasticsearchPoolUtil;
import ch.newsriver.website.WebSite;
import ch.newsriver.website.WebSiteFactory;
import ch.newsriver.website.WebsiteExtractor;
import ch.newsriver.executable.poolExecution.BatchInterruptibleWithinExecutorPool;
import ch.newsriver.util.http.HttpClientPool;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Created by eliapalme on 03/04/16.
 */
public class Intell extends BatchInterruptibleWithinExecutorPool implements Runnable{

    private static final Logger logger = LogManager.getLogger(Intell.class);
    private boolean run = false;
    private static int  MAX_EXECUTUION_DURATION = 120;
    private int batchSize;

    private static final ObjectMapper mapper = new ObjectMapper();
    Consumer<String, String> consumer;

    public Intell(int poolSize, int batchSize, int queueSize) throws IOException {

        super(poolSize, queueSize,Duration.ofSeconds(MAX_EXECUTUION_DURATION));
        this.batchSize = batchSize;
        run = true;

        try {
            HttpClientPool.initialize();
        } catch (NoSuchAlgorithmException e) {
            logger.fatal("Unable to initialize http connection pool", e);
            run = false;
            return;
        } catch (KeyStoreException e) {
            logger.error("Unable to initialize http connection pool", e);
            run = false;
            return;
        } catch (KeyManagementException e) {
            logger.error("Unable to initialize http connection pool", e);
            run = false;
            return;
        }

        Properties props = new Properties();
        InputStream inputStream = null;
        try {

            String propFileName = "kafka.properties";
            inputStream = Intell.class.getClassLoader().getResourceAsStream(propFileName);
            if (inputStream != null) {
                props.load(inputStream);
            } else {
                throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
            }
        } catch (Exception e) {
            logger.error("Unable to load kafka properties", e);
        } finally {
            try {
                inputStream.close();
            } catch (Exception e) {
            }
        }


        consumer = new KafkaConsumer(props);
        consumer.subscribe(Arrays.asList("website-url"));

    }

    public void stop() {
        run = false;
        HttpClientPool.shutdown();
        this.shutdown();
        consumer.close();
    }


    public void run() {

        while (run) {
            try {
                this.waitFreeBatchExecutors(batchSize);

                ConsumerRecords<String, String> records = consumer.poll(1000);
                IntellMain.addMetric("URLs in", records.count());
                for (ConsumerRecord<String, String> record : records) {

                    supplyAsyncInterruptExecutionWithin(() -> {

                        URI uri;
                        try {
                            uri = new URI(record.key());
                        }catch (URISyntaxException e){
                            logger.fatal("Invalid url",e);
                            return null;
                        }
                        WebSite webSite = WebSiteFactory.getInstance().getWebsite(uri.getHost().toLowerCase());
                        if(webSite == null){

                            webSite = new WebsiteExtractor().extract(uri.toString());
                            if(webSite !=null){
                                Client client = null;
                                client = ElasticsearchPoolUtil.getInstance().getClient();

                                try {
                                    IndexRequest indexRequest = new IndexRequest("newsriver-website", "website", webSite.getHostName().toLowerCase().trim());
                                    indexRequest.source(mapper.writeValueAsString(webSite));
                                    client.index(indexRequest).actionGet();
                                } catch (Exception e) {
                                    logger.error("Unable to save publisher", e);
                                } finally {
                                }

                                //update article if needed
                                if(record.value()!=null && !record.value().isEmpty()){
                                       /* try {
                                            UpdateRequest updateRequest = new UpdateRequest();
                                            updateRequest.index("newsriver");
                                            updateRequest.type("article");
                                            updateRequest.id(record.value());
                                            updateRequest.doc(jsonBuilder()
                                                    .startObject()
                                                    .field("website", webSite)
                                                    .endObject());
                                            client.update(updateRequest).get();

                                        } catch (Exception e) {
                                            logger.fatal("Unable to deserialize website", e);
                                            return null;
                                        }*/
                                }
                            }
                        }

                        return null;
                    }, this)
                            .exceptionally(throwable -> {
                                logger.error("Unrecoverable error.", throwable);
                                return null;
                            });

                }
            } catch (InterruptedException ex) {
                logger.warn("Miner job interrupted", ex);
                run = false;
                return;
            } catch (BatchSizeException ex) {
                logger.fatal("Requested a batch size bigger than pool capability.");
            }
            continue;
        }


    }
}
