package com.txs.springkafka.javakafka.producerinterceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * 计算发送成功和失败消息的次数
 */
public class CountInterceptor implements ProducerInterceptor<String,String> {

    public int successcount=0;
    public int failcount=0;

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        // 不做任何操作直接返回
        return record;
    }

    //生产者发送后返回调用
    //返回成功exception为空，失败不为空
    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {

        if(null ==exception)
        {
            //返回成功
            successcount++;
        }else
        {
            failcount++;
        }
    }

    @Override
    public void close() {

        //关闭生产者调用
        System.out.println("返回成功次数："+successcount);
        System.out.println("返回失败次数："+failcount);
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
