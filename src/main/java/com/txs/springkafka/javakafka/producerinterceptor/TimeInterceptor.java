package com.txs.springkafka.javakafka.producerinterceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * 生产者发送前给发送消息加时间戳
 */
public class TimeInterceptor implements ProducerInterceptor<String,String> {

    //发送时调用
    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
       //获取发送消息
        String oldvalue=record.value();
        //获取时间戳
        long time=System.currentTimeMillis();
        String newvlue=time+oldvalue;


        return new ProducerRecord<String, String>(record.topic(),newvlue);
    }

    //返回响应时调用
    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {

    }

    //关闭时调用
    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
