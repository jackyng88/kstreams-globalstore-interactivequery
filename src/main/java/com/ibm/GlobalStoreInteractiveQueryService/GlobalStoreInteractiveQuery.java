package com.ibm.GlobalStoreInteractiveQueryService;

import java.util.List;
import java.util.stream.Collectors;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.StreamsMetadata;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serdes;

import org.jboss.logging.Logger;

import com.ibm.Model.FinancialMessage;
import com.ibm.Topology.GlobalStoreTopology;

@ApplicationScoped
public class GlobalStoreInteractiveQuery {

    @Inject
    KafkaStreams streams;

    @ConfigProperty(name = "HOSTNAME")
    String host;

    private static final Logger LOG = Logger.getLogger(GlobalStoreInteractiveQuery.class);


    public List<PipelineMetadata> getMetaData() {
        return streams.allMetadataForStore(GlobalStoreTopology.GLOBAL_STORE)
                .stream()
                .map(m -> new PipelineMetadata(
                        m.hostInfo().host() + ":" + m.hostInfo().port(),
                        m.topicPartitions()
                                .stream()
                                .map(TopicPartition::toString)
                                .collect(Collectors.toSet())))
                .collect(Collectors.toList());
    }

    public FinancialMessageDataResult getFinancialMessageData(String user_id) {
        StreamsMetadata metadata = streams.metadataForKey(
                GlobalStoreTopology.GLOBAL_STORE,
                user_id,
                Serdes.String().serializer());

        if (metadata == null || metadata == StreamsMetadata.NOT_AVAILABLE) {
            LOG.warnv("Found no metadata for key {0}", user_id);
            return FinancialMessageDataResult.notFound();
        } else if (metadata.host().equals(host)) {
            LOG.infov("Found data for key {0} locally", user_id);
            FinancialMessage result = getGlobalStore().get(user_id);

            if (result != null) {
                return FinancialMessageDataResult.found(FinancialMessage.from(result));
            } else {
                return FinancialMessageDataResult.notFound();
            }
        } else {
            LOG.infov("Found data for key {0} on remote host {1}:{2}", user_id, metadata.host(), metadata.port());
            return FinancialMessageDataResult.foundRemotely(metadata.host(), metadata.port());
        }
    }

    private ReadOnlyKeyValueStore<String, FinancialMessage> getGlobalStore() {
        while (true) {
            try {
                return streams.store(GlobalStoreTopology.GLOBAL_STORE, QueryableStoreTypes.keyValueStore());
            } catch (InvalidStateStoreException e) {
                // ignore, store not ready yet
            }
        }
    }
}

    
