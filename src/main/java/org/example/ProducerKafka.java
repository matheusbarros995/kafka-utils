package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

public class ProducerKafka {

    private static final String TOPIC = "...";
    private static final String BOOTSTRAP_SERVERS = "...";
    private static final String USERNAME = "...";
    private static final String PASSWORD = "...";

    private static final int QTD_MENSAGENS = 1; // Número total de mensagens
    private static final int THREADS = 10; // Número de threads para paralelismo

    public static void main(String[] args) throws InterruptedException {
        // Configurações do Kafka Producer
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", BOOTSTRAP_SERVERS);
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        properties.setProperty("acks", "all");
        properties.setProperty("linger.ms", "5");
        properties.setProperty("batch.size", "32768");
        properties.setProperty("compression.type", "gzip");
        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty("sasl.mechanism", "SCRAM-SHA-512");
        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username='" + USERNAME + "' password='" + PASSWORD + "';");

        Producer<String, String> producer = new KafkaProducer<>(properties);

        // Carrega o JSON inicial
        JSONObject baseMessage = readJsonFile("mensagemKafka.json");

        // Executor para paralelismo
        ExecutorService executor = Executors.newFixedThreadPool(THREADS);
        CountDownLatch latch = new CountDownLatch(QTD_MENSAGENS);

        // Marca o início do tempo
        long startTime = System.nanoTime();

        // Enviar mensagens
        for (int i = 0; i < QTD_MENSAGENS; i++) {
            executor.submit(() -> {
                try {
                    String novoUuid = UUID.randomUUID().toString();

                    // Atualiza o JSON com o novo UUID
                    JSONObject kafkaMessage = new JSONObject(baseMessage.toString());
                    kafkaMessage.getJSONObject("event_data").put("transaction_uuid", novoUuid);

                    // Partição aleatória
                    int randomPartition = ThreadLocalRandom.current().nextInt(0, 10);

                    // Partição específica | Ex: partition 0
//                    int partition = 0;

                    // Cria e envia mensagem
                    String key = "mzaQd/hrAGIqjHk1X91PNp7JT4k=";
                    ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, randomPartition, key, kafkaMessage.toString());
//                    ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, partition, key, kafkaMessage.toString());

                    // Adiciona os headers
                    Headers headers = record.headers();
                    headers.add(new RecordHeader("AuthorizerResponseDetail", "APPROVED".getBytes()));

                    producer.send(record, (RecordMetadata metadata, Exception e) -> {
                        if (e != null) {
                            System.err.println("Error sending the message: " + e.getMessage());
                        } else {
                            System.out.printf("Message %s sent! Topic: %s, Partition: %d, Offset: %d%n",
                                    novoUuid, metadata.topic(), metadata.partition(), metadata.offset());
                        }
                        latch.countDown();
                    });

                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }

        // Aguarda o término de todas as mensagens
        latch.await();
        executor.shutdown();

        // Marca o término do tempo
        long endTime = System.nanoTime();

        // Calcula e imprime o tempo total em segundos
        long elapsedTime = endTime - startTime;
        System.out.printf("Tempo total para enviar %d mensagens: %.2f segundos%n",
                QTD_MENSAGENS, elapsedTime / 1_000_000_000.0);

        producer.close();
        System.out.println("Envio concluído!");
    }

    // Método para ler um arquivo JSON dentro da pasta /resources
    public static JSONObject readJsonFile(String jsonFileName) {
        try {
            InputStream inputStream = ProducerKafka.class.getClassLoader().getResourceAsStream(jsonFileName);
            if (inputStream == null) throw new IllegalArgumentException("File not found: " + jsonFileName);

            String content = new BufferedReader(new InputStreamReader(inputStream))
                    .lines()
                    .collect(Collectors.joining("\n"));
            return new JSONObject(content);
        } catch (Exception e) {
            System.err.println("Error processing JSON file: " + e.getMessage());
        }
        return null;
    }
}
