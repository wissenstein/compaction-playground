package com.ubs.o5d.kafkaforingestion;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static java.util.Arrays.asList;

public class CompactionPlayground {

    private static final String TOPIC = "compaction-test";

    private static final String KAFKA_HOST = "127.0.0.1:9092";

    private static final String MESSAGE_KEY = "123";

    private static final long NEW_SEGMENT_MS = 100L;
    private static final long LOG_CLEANER_BACKOFF_MS = 15000L;
    private static final String GROUP_ID = "compaction-test";

    private final KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps(KAFKA_HOST));
    private final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProperties(KAFKA_HOST));

    public void run() throws InterruptedException {

        sendMessage("1");
        waitForSomething(NEW_SEGMENT_MS);
        sendMessage("1");
        waitForSomething(LOG_CLEANER_BACKOFF_MS);

        // consuming one message with value "1"
        System.out.println("1st consume: ");
        System.out.println(consumeAll());

        sendMessage("2");
        waitForSomething(NEW_SEGMENT_MS);
        sendMessage("2");
        waitForSomething(LOG_CLEANER_BACKOFF_MS);

        // consuming one message with value "2"
        System.out.println("2nd consume: ");
        System.out.println(consumeAll());

        sendMessage(null);
        waitForSomething(NEW_SEGMENT_MS);
        sendMessage(null);
        waitForSomething(LOG_CLEANER_BACKOFF_MS);

        // consuming one message with null value - should be deleted?
        System.out.println("3nd consume: ");
        System.out.println(consumeAll());
    }

    private void sendMessage(String payload) {
        Future<RecordMetadata> future = producer.send(new ProducerRecord<>(TOPIC, MESSAGE_KEY, payload));

        try {
            future.get();
        } catch (InterruptedException ignore) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    public List<ConsumerRecord<String, String>> consumeAll(){
        TopicPartition partition = new TopicPartition(TOPIC, 0);
        consumer.assign(asList(partition));
        consumer.seekToBeginning(asList(partition));

        ConsumerRecords<String, String> records = consumer.poll(1000L);

        return StreamSupport.stream(records.spliterator(), false).collect(Collectors.toList());
    }

    public static void main(String[] args) throws InterruptedException {
        CompactionPlayground compactionPlayground = new CompactionPlayground();
        compactionPlayground.run();
        compactionPlayground.shutdown();
    }

    private void shutdown() {
        producer.close();
        consumer.commitSync();
        consumer.close();
    }

    private void waitForSomething(long timeInMillis) throws InterruptedException {
        Thread.sleep(timeInMillis);
    }

    private Properties producerProps(String kafkaHost) {

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaHost);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 0); // immediately send out
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        return props;
    }

    private Properties consumerProperties(String kafkaHost) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaHost);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "200");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        return props;
    }
}
