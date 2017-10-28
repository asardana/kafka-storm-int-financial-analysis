package com.financial.kafka.storm.integration;

import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.tuple.Fields;

import java.util.Arrays;

/**
 * Created by Aman on 10/27/2017.
 */
public class LoanDataKafkaSpout {

    /**
     * Creates the Loan Data Kafka spout for reading the raw loan data records from the kafka Broker
     * Converts the Kafka ConsumerRecord to a Tuple (Offset, Loan Record Key, Loan Record Value)
     *
     * @param kafkaBrokerEndpoint
     * @param rawLoanDataInputTopic
     * @return
     */
    public static KafkaSpout createLoanDataKafkaSpout(String kafkaBrokerEndpoint, String rawLoanDataInputTopic) {

        KafkaSpoutConfig loanDataSpoutConfig = KafkaSpoutConfig.builder(kafkaBrokerEndpoint, rawLoanDataInputTopic)
                .setGroupId("loan_data_storm_spout")
                .setFirstPollOffsetStrategy(KafkaSpoutConfig.FirstPollOffsetStrategy.UNCOMMITTED_EARLIEST)
                .setRecordTranslator(consumerRecord -> {
                    return Arrays.asList(consumerRecord.offset(), consumerRecord.key(), consumerRecord.value());
                }, new Fields(LoanDataCleansingContants.LOAN_DATA_TUPLE_OFFSET, LoanDataCleansingContants.LOAN_DATA_TUPLE_KEY, LoanDataCleansingContants.LOAN_DATA_TUPLE_LOANRECORD))
                .build();

        return new KafkaSpout<>(loanDataSpoutConfig);
    }
}
