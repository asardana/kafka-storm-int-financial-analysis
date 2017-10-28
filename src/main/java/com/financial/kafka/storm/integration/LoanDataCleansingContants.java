package com.financial.kafka.storm.integration;

/**
 * Created by Aman on 10/27/2017.
 */
public class LoanDataCleansingContants {

    public static String LOAN_DATA_CLEANSING_SPOUT = "loan_data_spout";
    public static String LOAN_DATA_CLEANSING_BOLT = "loan_data_cleansing_bolt";
    public static String LOAN_DATA_CLEANSED_WRITER_BOLT = "loan_data_cleansed_writer_bolt";
    public static String LOAN_DATA_TUPLE_KEY = "key";
    public static String LOAN_DATA_TUPLE_LOANRECORD = "loanrecord";
    public static String LOAN_DATA_TUPLE_OFFSET = "offset";
    public static String LOAN_DATA_SCRUBBED_TUPLE_LOANRECORD = "scrubbed_loan_data_record";
}
