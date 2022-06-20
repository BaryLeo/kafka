package org.apache.kafka.demo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * 类描述
 *
 * @author razor.liu
 * @version 1.0
 * @date 2022/6/15 7:41 PM
 */
public class KafkaProducerDemo {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaProducerDemo.class);

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        //1.加载配置信息
        Properties prop = loadProperties();

        //2.创建生产者
        KafkaProducer<String,String> producer = new KafkaProducer<>(prop);

        String sendContent = "hello_kafka";
        ProducerRecord<String,String> record = new ProducerRecord<>("hello_kafka",sendContent);

        Future<RecordMetadata> future = producer.send(record);

        RecordMetadata recordMetadata = future.get();

        LOG.info("发送的数据是 {},offset 是{}",sendContent,recordMetadata.offset());

    }

    //配置文件的设置
    public static Properties loadProperties() {
        Properties prop = new Properties();
        prop.put("bootstrap.servers", "localhost:9092");
        prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        return prop;
    }
}
