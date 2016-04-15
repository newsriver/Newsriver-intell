import ch.newsriver.dao.JDBCPoolUtil;
import ch.newsriver.dao.RedisPoolUtil;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.impl.client.SystemDefaultCredentialsProvider;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;

/**
 * Created by eliapalme on 05/04/16.
 */
public class TestFixRedisAlexa {


    @Test
    public void uploadKnowDomains() throws Exception {

        HashSet<String> found = new HashSet<String>();
            try (Jedis jedis = RedisPoolUtil.getInstance().getResource(RedisPoolUtil.DATABASES.ALEXA_CACHE)) {
                String next ="0";
                ScanParams scanparams = new ScanParams();
                scanparams.match("alexaStite:1:*");
                scanparams.count(200);
                do {
                    try {
                        ScanResult<String> result = jedis.scan(next, scanparams);
                        System.out.println("");
                        System.out.println("------------------------------------------");
                        System.out.println("");
                        next = result.getStringCursor();

                        result.getResult().parallelStream().forEach(key -> {
                            try {
                                List<NameValuePair> params = URLEncodedUtils.parse(new URI(key.substring(13, key.length())), "UTF-8");
                                NameValuePair param = params.get(params.size() - 2);
                                try (Jedis jedis2 = RedisPoolUtil.getInstance().getResource(RedisPoolUtil.DATABASES.ALEXA_CACHE)) {
                                    if (found.contains(param.getValue())) {
                                        jedis2.del(key);
                                    } else {
                                        jedis2.rename(key, "alexaStite:2:" + new URI(param.getValue()).getHost().toLowerCase().trim());
                                        found.add(param.getValue());
                                    }
                                } catch (JedisConnectionException e) {
                                    e.printStackTrace();
                                }

                                //System.out.println(param.getValue());
                            } catch (URISyntaxException e) {
                            }

                        });
                    }catch (JedisConnectionException e){
                        e.printStackTrace();
                    }

                }while (!next.equalsIgnoreCase("0"));

            }




    }

}
