package com.spring.greencampus.ems;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Arrays;
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
        config.put("bootstrap.servers", "127.0.0.1:2097");
        config.put("client.id", "RawDataProducer");
        config.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        config.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<Integer, String> producer = new KafkaProducer<Integer, String>(config);
        int msgCount = 0;
        while(msgCount<1000){
            Thread.sleep(1000);
            try{
                producer.send(new ProducerRecord<>("raw-data-input", getSmartmeterId(), getRawData())).get();
                System.out.println("Sent Message Success");
            }
            catch (InterruptedException|ExecutionException e){
                e.printStackTrace();
            }
            finally {
                msgCount++;
            }
        }
        System.out.println("This is the end of sending messages.");
//
//        KStreamBuilder builder = new KStreamBuilder();
//        // 1 - stream from Kafka
//        KStream<Integer, String> kv =
//
//        KStream<String, String> textLines = builder.stream("raw-data-input");
//        KTable<String, Long> wordCounts = textLines
//                // 2 - map values to lowercase
//                .mapValues(textLine -> textLine.toLowerCase())
//                // can be alternatively written as:
//                // .mapValues(String::toLowerCase)
//                // 3 - flatmap values split by space
//                .flatMapValues(textLine -> Arrays.asList(textLine.split("\\W+")))
//                // 4 - select key to apply a key (we discard the old key)
//                .selectKey((key, word) -> word)
//                // 5 - group by key before aggregation
//                .groupByKey()
//                // 6 - count occurences
//                .count("Counts");
//
//        // 7 - to in order to write the results back to kafka
//        wordCounts.to(Serdes.String(), Serdes.Long(), "word-count-output");
//
//        KafkaStreams streams = new KafkaStreams(builder, config);
//        streams.start();
//
//        // print the topology
//        System.out.println(streams.toString());
//
//        // shutdown hook to correctly close the streams application
//        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
//        System.out.println("This is a test");
    }

}
