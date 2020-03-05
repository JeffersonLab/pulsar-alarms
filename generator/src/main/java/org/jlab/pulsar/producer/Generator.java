package org.jlab.pulsar.producer;

import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;

import org.jlab.pulsar.schema.Alarm;

import java.io.IOException;

public class Generator {

    public void start() throws IOException, InterruptedException {
        String pulsarUrl = System.getenv("PULSAR_URL");

        if(pulsarUrl == null) {
            throw new IOException("Environment variable PULSAR_URL not found");
        }

        try(PulsarClient client = PulsarClient.builder()
                .serviceUrl(pulsarUrl)
                .build()) {

            Producer<Alarm> producer = client.newProducer(Schema.AVRO(Alarm.class))
                    .topic("alarms")
                    .create();

            long i = 0;

            while (true) {
                String pv = "testing";
                short severity = 2;
                String value = "Hi " + i++;
                producer.send(new Alarm(pv, severity, value));
                Thread.sleep(1000);
            }
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        Generator gen = new Generator();

        gen.start();
    }
}
