package com.spring.greencampus.ems;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;

public class RawDataGenerator {
    static final int SMARTMETERS = 30;
    static Random randData = new Random();
    static Random randId = new Random();
    public static int getSmartmeterId(){
        return(randId.nextInt(SMARTMETERS));
    }
    public static String getRawData(){
        int flag = randData.nextInt(3);
        String res = "";
        if(flag == 0){
            res = "0,"+String.valueOf(randData.nextFloat())+",1,"+String.valueOf(randData.nextFloat());
        }
        else if(flag == 1){
            res = "2,"+String.valueOf(randData.nextFloat());
        }
        else{
            res = "3,"+String.valueOf(randData.nextFloat());
        }
        return res;

    }
    public static void main(String[] args) throws InterruptedException {
        Properties config = new Properties();
        config.put("bootstrap.servers", "127.0.0.1:7092");
        config.put("client.id", "RawDataProducer");
        config.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        config.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<Integer, String> producer = new KafkaProducer<Integer, String>(config);
        int msgCount = 0;
        while(msgCount<1000){
            Thread.sleep(1000);
            try{
                producer.send(new ProducerRecord<>("raw-data-input", getSmartmeterId(), getRawData())).get();
//                System.out.println("Sent Message Success");
            }
            catch (InterruptedException|ExecutionException e){
                e.printStackTrace();
            }
            finally {
                msgCount++;
            }
        }
        System.out.println("This is the end of sending messages.");

    }

}
