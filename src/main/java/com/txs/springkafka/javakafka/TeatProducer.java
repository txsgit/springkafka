package com.txs.springkafka.javakafka;

import org.apache.kafka.clients.producer.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class TeatProducer {


    public static void main(String[] args) {

        //配置数据
        Properties properties=new Properties();
        properties.setProperty("bootstrap.servers","192.168.31.206:9092");

        properties.setProperty("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("value.serializer","org.apache.kafka.common.serialization.StringSerializer");

        //添加拦截器配置
        List<String> interceptors=new ArrayList<>();
        interceptors.add("com.txs.springkafka.javakafka.producerinterceptor.TimeInterceptor");
        interceptors.add("com.txs.springkafka.javakafka.producerinterceptor.CountInterceptor");
        properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,interceptors);
        //创建生产者
        Producer producer=new KafkaProducer<String,String>(properties);

        //准备数据
        String topic ="test";
        String value="hello";
//        ProducerRecord<String, String> record=new ProducerRecord(topic,value);
        //指定分区
        ProducerRecord<String, String> record=new ProducerRecord(topic,1,null,value);



        //发送数据
//        producer.send(record);
        //异步回调
        //调用多次测试拦截器
        for(int i=0 ; i<5; i++)
        {
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    System.out.println(metadata);
                    //发送的topic
                    System.out.println(metadata.topic());
                    //发送哪个分区
                    System.out.println(metadata.partition());
                    //发送数据偏移量
                    System.out.println(metadata.offset());
                }
            });

        }

        //关闭生产者
        producer.close();
    }
}
