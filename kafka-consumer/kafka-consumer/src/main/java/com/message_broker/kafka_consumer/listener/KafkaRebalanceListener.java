package com.message_broker.kafka_consumer.listener;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ConsumerAwareRebalanceListener;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@Component
public class KafkaRebalanceListener implements ConsumerAwareRebalanceListener {


    @Override
    public void onPartitionsRevokedBeforeCommit(
            Consumer<?, ?> consumer,
            Collection<TopicPartition> partitions) {

        if (partitions.isEmpty()) return;

        log.warn("Partitions revoked: {}", partitions);
        try {
            Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
            for (TopicPartition partition : partitions) {
                long position = consumer.position(partition); // ← real consumer, works correctly
                currentOffsets.put(partition, new OffsetAndMetadata(position));
                log.info("Saving position {} for partition {} before rebalance",
                        position, partition);
            }
            consumer.commitSync(currentOffsets);
            log.info("Successfully committed offsets for revoked partitions");
        } catch (Exception e) {
            log.error("Failed to commit offsets during rebalance", e);
        }
    }

    @Override
    public void onPartitionsAssigned(Consumer<?, ?> consumer,
                                     Collection<TopicPartition> partitions) {
        log.info("Partitions assigned: {}", partitions);
        long oneHourAgo = System.currentTimeMillis() - 3600000;
        Map<TopicPartition, Long> timestampsToSearch = new HashMap<>();
        for (TopicPartition partition : partitions) {
            timestampsToSearch.put(partition, oneHourAgo);
        }

        Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes =
                consumer.offsetsForTimes(timestampsToSearch);

        for (Map.Entry<TopicPartition, OffsetAndTimestamp> entry : offsetsForTimes.entrySet()) {
            if (entry.getValue() != null) {
                consumer.seek(entry.getKey(), entry.getValue().offset());
                log.info("Seek to timestamp: partition {} to offset {}",
                        entry.getKey().partition(), entry.getValue().offset());
            }
        }
    }

    @Override
    public void onPartitionsLost(
            Consumer<?, ?> consumer,
            Collection<TopicPartition> partitions) {

        log.error("Partitions lost unexpectedly: {}", partitions);
    }
}
