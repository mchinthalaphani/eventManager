package com.charter.kafka.service.config;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

@SuppressWarnings("UnusedDeclaration")
public class KafkaPartitioner implements Partitioner {
    public KafkaPartitioner(VerifiableProperties properties) 
    {
    }

    public int partition(Object key, int numberOfPartitions) 
    {
        int partition = 0;
        int intKey = (Integer) key;
        if (intKey > 0) {
            partition = intKey % numberOfPartitions;
        }
        return partition;
    }
}
