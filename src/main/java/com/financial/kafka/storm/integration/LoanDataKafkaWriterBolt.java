package com.financial.kafka.storm.integration;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.bolt.selector.DefaultTopicSelector;

import java.util.Properties;

/**
 * Created by Aman on 10/27/2017.
 */
public class LoanDataKafkaWriterBolt {

    /**
     * Creates the Bolt for writing the cleansed loan data records back to the the Kafka broker.
     * Creates the Bolt for writing the cleansed loan data records back to the the Kafka broker.
     * Maps the Tuple fields to the key-value pair in a Kafka ProducerRecord
     *
     * @param kafkaBrokerEndpoint
     * @param cleansedLoanDataOutputTopic
     * @return
     */
    public static KafkaBolt createLoanDataKafkaWriterBolt(String kafkaBrokerEndpoint, String cleansedLoanDataOutputTopic) {

        KafkaBolt<String, String> cleansedLoanDataWriterBolt = new KafkaBolt<String, String>().withProducerProperties(getKafkaProducerProperties(kafkaBrokerEndpoint))
                .withTopicSelector(new DefaultTopicSelector(cleansedLoanDataOutputTopic))
                .withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper<String, String>(LoanDataCleansingContants.LOAN_DATA_TUPLE_KEY, LoanDataCleansingContants.LOAN_DATA_SCRUBBED_TUPLE_LOANRECORD));

        return cleansedLoanDataWriterBolt;
    }

    private static Properties getKafkaProducerProperties(String kafkaBrokerEndpoint) {

        Properties kafkaProducerProp = new Properties();

        kafkaProducerProp.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokerEndpoint);
        kafkaProducerProp.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaProducerProp.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaProducerProp.put(ProducerConfig.CLIENT_ID_CONFIG, "loan_data_storm_bolt");

        return kafkaProducerProp;
    }
}
