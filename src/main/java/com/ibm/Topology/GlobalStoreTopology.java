package com.ibm.Topology;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import io.quarkus.kafka.client.serialization.JsonbSerde;

import com.ibm.Model.FinancialMessage;

@ApplicationScoped
public class GlobalStoreTopology {

    @ConfigProperty(name = "KTABLE_TOPIC_NAME")
    private String KTABLE_TOPIC;

    public static final String GLOBAL_STORE = "globalStore";

    @Produces
    public Topology buildTopology() {

        StreamsBuilder builder = new StreamsBuilder();
        KeyValueBytesStoreSupplier globalStoreSupplier = Stores.persistentKeyValueStore(GLOBAL_STORE);
        JsonbSerde<FinancialMessage> financialMessageSerde = new JsonbSerde<>(FinancialMessage.class);

        GlobalKTable<String, FinancialMessage> globalStore = 
            builder.globalTable(
                KTABLE_TOPIC,
                Consumed.with(Serdes.String(), financialMessageSerde),
                Materialized.<String, FinancialMessage> as(globalStoreSupplier)
                            .withKeySerde(Serdes.String())
                            .withValueSerde(financialMessageSerde)
            );
        

        // builder.addStateStore(globalStoreSupplier);
        // builder.addGlobalStore(storeBuilder, topic, consumed, stateUpdateSupplier)

        // KStream<String, FinancialMessage> sampleStream = builder.stream(
        //     KTABLE_TOPIC,
        //     Consumed.with(Serdes.String(), financialMessageSerde));

        // sampleStream.peek((key, value) -> System.out.println("Value in topic, " + value));

        // ReadOnlyKeyValueStore<String, Long> keyValueStore =
        //         builder.store(GLOBAL_STORE, QueryableStoreTypes.keyValueStore());

        // KeyValueStore<String,FinancialMessage> store = builder.getStateStore(GLOBAL_STORE);

        // System.out.println("count for hello:" + globalStoreSupplier.get("hello"));
        
        
        return builder.build();
    }

    
    
}