package org.jlab.pulsar.schema;

public class Alarm {
    String name;
    short severity;
    String value;

    public Alarm() {
        // AVRO requires no-arg constructor
    }

    public Alarm(String name, short severity, String value) {
        this.name = name;
        this.severity = severity;
        this.value = value;
    }

    public String toString() {
        return "pv: " + name + ", severity: " + severity + ", value: " + value;
    }
}
