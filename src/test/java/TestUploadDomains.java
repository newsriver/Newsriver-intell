import ch.newsriver.dao.JDBCPoolUtil;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

/**
 * Created by eliapalme on 05/04/16.
 */
public class TestUploadDomains {


    @Test
    public void uploadKnowDomains() throws Exception {

        Producer<String, String> producer;
        Properties props = new Properties();
        InputStream inputStream = null;
        try {

            String propFileName = "kafka.properties";
            inputStream = TestUploadDomains.class.getClassLoader().getResourceAsStream(propFileName);
            if (inputStream != null) {
                props.load(inputStream);
            } else {
                throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
            }
        } finally {
            try {
                inputStream.close();
            } catch (java.lang.Exception e) {
            }
        }

        producer = new KafkaProducer(props);



        String sql = "Select * from Newsriver.host";
        try (Connection conn = JDBCPoolUtil.getInstance().getConnection(JDBCPoolUtil.DATABASES.Sources); PreparedStatement stmt = conn.prepareStatement(sql);) {
            int cnt=0;
            try (ResultSet rs = stmt.executeQuery();) {
                while (rs.next()) {
                    producer.send(new ProducerRecord<String, String>("website-url", "http://" + rs.getString("hostname"),""));
                    cnt++;
                    if(cnt==500){
                        System.out.println("500");
                        cnt=0;
                        Thread.sleep(20*1000);
                    }
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

}
