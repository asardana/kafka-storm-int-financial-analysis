package com.financial.kafka.storm.integration;

/**
 * Created by Aman on 10/27/2017.
 */
public class LoanDataCleansingStormMain {

    private static String kafkaBrokerEndpoint = null;
    private static String rawLoanDataInputTopic = null;
    private static String cleansedLoanDataOutputTopic = null;


    public static void main(String args[]) {

        if (args != null) {

            // Read command Line Arguments for Kafka broker, Topic for reading the raw Loan Records from Kafka Topic and the
            // Topic for publishing the cleansed records back to the Kafka
            kafkaBrokerEndpoint = args[0];
            rawLoanDataInputTopic = args[1];
            cleansedLoanDataOutputTopic = args[2];
        }

        // Create the Loan Data Cleansing Topology and run the Topology
        LoanDataCleansingTopology.createAndRunLoanDataCleansingTopology(kafkaBrokerEndpoint, rawLoanDataInputTopic, cleansedLoanDataOutputTopic);
    }

}
