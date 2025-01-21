package org.example;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.json.JSONObject;
import tech.dock.Consumer;

public class ConsumerKafka {

    private static final String TOPIC = "...";
    private static final String BOOTSTRAP_SERVERS = "...";
    private static final String USERNAME = "...";
    private static final String PASSWORD = "...";

    public static void main(String[] args) {

        Consumer consumer = new Consumer(USERNAME, PASSWORD, TOPIC, BOOTSTRAP_SERVERS);

        // Procura uma mensagem que contenha o texto abaixo
        String mensagem = "\"transaction_uuid\":\"0ee737fe-a684-4676-9d16-3f52ef1eafb3\""; // Exemplo de busca pelo uuid da transacao
        // String mensagem = "\"nsu\":\"608423\""; // Exemplo de busca pelo nsu

        ConsumerRecord<String, String> record = consumer.searchMessageInPartitions(
                mensagem,
                20, // Maximo de mensagens a ser buscada por particao
                60); // Tempo maximo da busca | 60 = Vai procurar a mensagem durante no maximo 60 segundos

        // Print dos headers
        System.out.println(convertHeadersToString(record.headers()));
        // Print do conteudo da mensagem
        System.out.println(new JSONObject(record.value()).toString(4));
    }

    public static StringBuilder convertHeadersToString(Headers headers) {
        // Iterar pelos headers e converter para String
        StringBuilder headersAsString = new StringBuilder("Headers:\n");
        for (Header header : headers) {
            String key = header.key();
            String value = new String(header.value()); // Decodifica byte[] para String
            headersAsString.append(key).append(" = ").append(value).append("\n");
        }

        return headersAsString;
    }
}
