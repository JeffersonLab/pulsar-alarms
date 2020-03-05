package org.jlab.pulsar.consumer;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;

import org.jlab.pulsar.schema.Alarm;

import java.io.IOException;
import java.util.*;

public class Subscriber {

    public void start() throws IOException {
        String pulsarUrl = System.getenv("PULSAR_URL");

        if(pulsarUrl == null) {
            throw new IOException("Environment variable PULSAR_URL not found");
        }

        try(PulsarClient client = PulsarClient.builder()
                .serviceUrl(pulsarUrl)
                .build()) {

            List<String> topicList = Arrays.asList("alarms");

            Consumer<Alarm> consumer = client.newConsumer(Schema.AVRO(Alarm.class))
                    .topics(topicList)
                    .subscriptionName(UUID.randomUUID().toString())
                    .subscribe();

            while(true) {
                Message<Alarm> msg = consumer.receive();

                Alarm alarm = msg.getValue();

                System.out.println(msg.getTopicName() + ": " + alarm.toString());

                consumer.acknowledge(msg);
            }

        }
    }

    public static void main(String[] args) throws IOException {
        Subscriber sub = new Subscriber();

        sub.start();
    }
}
