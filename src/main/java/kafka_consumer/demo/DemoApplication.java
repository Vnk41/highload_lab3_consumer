package kafka_consumer.demo;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

import java.util.*;
import java.time.Duration;

@EnableKafka
@SpringBootApplication
public class DemoApplication {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "testgroup");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        List<String> topics = new LinkedList<String>();
        topics.add("TutorialTopic");

        consumer.subscribe(Arrays.asList("TutorialTopic"));

        Map<String, List<PartitionInfo>> list = consumer.listTopics();
        System.out.println(consumer.listTopics());
        System.out.println(list.size());

        System.out.println("Subscribed to topic " + topics);
        int i = 0;

        //Duration timeout = 30;

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(50000));
            System.out.println(records.count());
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("topic = %s, key = %s, value = %s\n",
                        record.topic(), record.key(), record.value());
                System.out.println(record.offset());
            }

        }
    }


}
