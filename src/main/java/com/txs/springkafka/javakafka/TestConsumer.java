package com.txs.springkafka.javakafka;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;



public class TestConsumer {


     public static final Logger logger= LoggerFactory.getLogger(TestConsumer.class);

    public static void main(String[] args) {
        //使用哪个log控制kafka客户端打印日志级别
        LoggerContext loggerContext = (LoggerContext)LoggerFactory.getILoggerFactory();
        loggerContext.getLogger("org.apache.kafka.clients").setLevel(Level.ERROR);
        //配置数据
        Properties properties=new Properties();
        properties.setProperty("bootstrap.servers","192.168.31.206:9092");

        properties.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");

        properties.setProperty("group.id","testgroup");
        //创建消费者对象
        Consumer consumer=new KafkaConsumer(properties);

        //订阅主题
        consumer.subscribe(Arrays.asList("test"));

        //循环拉去消息
        while (true){

            ConsumerRecords<String, String> records= consumer.poll(Duration.ofMillis(500));
             for(ConsumerRecord<String, String> recoder:records)
             {
                 System.out.println(recoder);
                 System.out.println(recoder.topic());
                 System.out.println(recoder.value());
                 System.out.println(recoder.offset());
                 System.out.println(recoder.key());
             }
        }

    }
}
