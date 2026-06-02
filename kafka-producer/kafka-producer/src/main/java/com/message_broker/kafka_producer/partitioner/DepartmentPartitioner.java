package com.message_broker.kafka_producer.partitioner;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.utils.Utils;

import java.util.Map;

public class DepartmentPartitioner implements Partitioner {
    private final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(DepartmentPartitioner.class);

    @Override
    public int partition(String topic, Object key, byte[] keyBytes,
                         Object value, byte[] valueBytes, Cluster cluster) {

        int numPartitions = cluster.partitionCountForTopic(topic);

        if (numPartitions == 0) {
            throw new InvalidRecordException("Topic has no partitions: " + topic);
        }

        if (value instanceof schema.avro.AvroUser user) {
            String department = user.getDepartment().toString();
            return switch (department) {
                case "ENGINEERING" -> 0;
                case "SALES" -> 1 % numPartitions;
                case "HR" -> 2 % numPartitions;
                case "FINANCE" -> 3% numPartitions;
                default -> Math.abs(Utils.murmur2(keyBytes)) % numPartitions;
            };
        }

        if (keyBytes == null) {
            return 0;
        }
        return Math.abs(Utils.murmur2(keyBytes)) % numPartitions;
    }


    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

        log.info("DepartmentPartitioner configured");
    }
}
