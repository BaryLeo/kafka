package org.apache.kafka.demo;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemo {
    private static final Logger LOG = LoggerFactory.getLogger(ConsumerDemo.class);

    public static void main(String[] args) {
        //1.加载配置信息
        Properties prop = loadProperties();

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(prop);

        //1.订阅消息
        consumer.subscribe(Collections.singletonList("hello_kafka"));

        //2.读取消息
        for(;;) {

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100).toMillis());
            records.forEach(items->
                    {
                        LOG.info("===============> offset:{},value:{}",items.offset(),items.value());
                    }
            );
        }
    }

    private static Properties loadProperties() {
        Properties prop = new Properties();
        prop.put("bootstrap.servers", "localhost:9092");
        prop.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        prop.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        prop.put("group.id", "hello_2");
        prop.put("client.id", "demo-consumer-client");
        prop.put("auto.offset.reset", "earliest");        // earliest(最早) latest(最晚)

        return prop;
    }
}
