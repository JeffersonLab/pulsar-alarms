package org.jlab;

import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.functions.LocalRunner;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;
import org.apache.pulsar.functions.api.Record;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;

public class ConditionalExample implements Function<String, String> {
    /**
     * Process the input.
     *
     * @param input
     * @param context
     * @return the output
     */
    @Override
    public String process(String input, Context context) throws Exception {
        System.err.println("Input: " + input);

        Record record = context.getCurrentRecord();
        String topicName = ((Optional<String>)record.getTopicName()).get();

        System.err.println("incoming topic: " + topicName);

        //ByteBuffer previousBytes = context.getState("previous");

        return input + "123!";
    }
    /*public String process(String input, Context context) throws Exception {
        ByteBuffer previousBytes = context.getState("previous");
        context.putState("previous", ByteBuffer.wrap(input.getBytes(StandardCharsets.UTF_8)));

        String previous = "";
        if(previousBytes != null) {
            previous = new String(previousBytes.array(), StandardCharsets.UTF_8);
        }

        String result =  input + previous;

        System.err.println("Result: " + result);

        return result;
    }*/

    public static void main(String[] args) throws Exception {
        String pulsarUrl = System.getenv("PULSAR_URL");

        if(pulsarUrl == null) {
            throw new IOException("Environment variable PULSAR_URL not found");
        }

        FunctionConfig functionConfig = new FunctionConfig();
        functionConfig.setName("conditional-example2");
        functionConfig.setInputs(Arrays.asList("input1", "input2"));
        functionConfig.setRuntimeFlags("--state_storage_serviceurl bk://localhost:4181");
        functionConfig.setClassName(ConditionalExample.class.getName());
        functionConfig.setRuntime(FunctionConfig.Runtime.JAVA);
        functionConfig.setOutput("output");

        LocalRunner localRunner = LocalRunner.builder().functionConfig(functionConfig).brokerServiceUrl(pulsarUrl).build();
        localRunner.start(false);
    }
}
