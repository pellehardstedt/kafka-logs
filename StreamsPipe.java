import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.kafka.support.KafkaStreamBrancher;

import java.text.ParseException;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class StreamsPipe extends Thread{

    private static final String inputTopic = "logs-input";
    private static final String outputTopic = "logs-output";
    private static final String errorsTopic = "logs-output-errors";
    private static final String warningsTopic = "logs-output-warnings";

    private static int i;

    public StreamsPipe(){}

    public void run(){
        System.out.println(Thread.currentThread().getName() + " is running. (Kafka Streams)");
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-pipe");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        //props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092, localhost:9093, localhost:9094");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        //props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put("schema.registry.url", "http://localhost:8081");


        // Define the processing topology of the Streams application.
        final StreamsBuilder builder = new StreamsBuilder();
        conformStream(builder);
        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.cleanUp();

        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
    
    //Efter rad 66 kommer detta exception
    
    //ClassCastException invoking Processor. Do the Processor's input types match the deserialized types? Check the Serde setup and change the default Serdes inStreamConfig or provide correct Serdes via method parameters.
    //Make sure the Processor can accept the deserialized input of type key: java.lang.String, and value: java.lang.String.
    //Note that although incorrect Serdes are a common cause of error, the cast exception might have another cause (in user code, for example).
    //For example, if a processor wires in a store, but casts the generics incorrectly, a class cast exception could be raised during processing, but the cause would not be wrong Serdes.
    
    //java.lang.String cannot be cast to logSchema
    
    static void conformStream(final StreamsBuilder builder) {
        final KStream<String, String> stream = builder.stream(inputTopic);

        final KStream<String, logSchema> transformed = stream.map(
            (key, value) -> {
                try {
                    System.out.println("inside transformed");
                    return KeyValue.pair(key, new logSchema(value));
                } catch (ParseException e) {
                    e.printStackTrace();
                }
                return null;
            });

        new KafkaStreamBrancher<String, logSchema>()
            .branch((key, value) -> {
                try {
                    return errorCheck(value);
                } catch (ParseException e) {
                    e.printStackTrace();
                }
                return false;
            }, ks->ks.to(errorsTopic))
            .branch((key, value) -> {
                try {
                    return warningCheck(value);
                } catch (ParseException e) {
                    e.printStackTrace();
                }
                return false;
            }, ks->ks.to(warningsTopic))
            .defaultBranch(ks->ks.to(outputTopic))
            .onTopOf(builder.stream(inputTopic));


        //get schema from schema registry
        final Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url",
                "http://localhost:8081");

        final Serde<logSchema> valueSpecificAvroSerde = new SpecificAvroSerde<>();
        valueSpecificAvroSerde.configure(serdeConfig, false); // `false` for record values

        final Serde<String> stringSerde = Serdes.String();

        transformed.to(outputTopic, Produced.with(stringSerde, valueSpecificAvroSerde));
    }

    private static logSchema schemaTransform(String inputString) throws ParseException {
        System.out.println("inside schema transform: " + i);
        i = i +1;
        //logSchema log = new logSchema(123214214, "[notice]", "LDAP: Built with OpenLDAP LDAP SDK");
        logSchema log = new logSchema(inputString);
        System.out.println(log.getType());
        return log;
    }

    private static boolean errorCheck(logSchema log) throws ParseException {
        return log.getType() == "[error]";
    }
    private static boolean warningCheck(logSchema log) throws ParseException {
        return log.getType() == "[warning]";
    }
}
