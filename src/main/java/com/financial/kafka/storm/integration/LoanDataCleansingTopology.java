package com.financial.kafka.storm.integration;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

/**
 * Created by Aman on 10/27/2017.
 */
public class LoanDataCleansingTopology {

    /**
     * Creates the Loan Data Topology with one Spout and two Bolts
     *
     * @param kafkaBroker
     * @param rawLoanDataInputTopic
     * @param cleansedLoanDataOutputTopic
     */
    public static void createAndRunLoanDataCleansingTopology(String kafkaBroker, String rawLoanDataInputTopic, String cleansedLoanDataOutputTopic) {

        TopologyBuilder loanDataTopologyBuilder = new TopologyBuilder();

        // Spout to read the raw loan data from Kafka broker
        loanDataTopologyBuilder.setSpout(LoanDataCleansingContants.LOAN_DATA_CLEANSING_SPOUT, LoanDataKafkaSpout.createLoanDataKafkaSpout(kafkaBroker, rawLoanDataInputTopic), 1);
        // Bolt to cleanse the loan data record by dropping the invalid records and reformatting the records based on RegEx pattern
        loanDataTopologyBuilder.setBolt(LoanDataCleansingContants.LOAN_DATA_CLEANSING_BOLT, new LoanDataCleansingBolt(), 1).shuffleGrouping(LoanDataCleansingContants.LOAN_DATA_CLEANSING_SPOUT);
        // Bolt to publish the cleansed loan data record back to Kafka
        loanDataTopologyBuilder.setBolt(LoanDataCleansingContants.LOAN_DATA_CLEANSED_WRITER_BOLT, LoanDataKafkaWriterBolt.createLoanDataKafkaWriterBolt(kafkaBroker, cleansedLoanDataOutputTopic)).shuffleGrouping(LoanDataCleansingContants.LOAN_DATA_CLEANSING_BOLT);

        LocalCluster localCluster = new LocalCluster();
        Config conf = new Config();
        conf.setDebug(false);
        // submit the topology to local cluster
        localCluster.submitTopology("LoanDataCleansingTopology", conf, loanDataTopologyBuilder.createTopology());
    }
}
