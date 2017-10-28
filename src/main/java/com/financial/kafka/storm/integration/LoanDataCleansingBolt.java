package com.financial.kafka.storm.integration;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * Created by Aman on 10/27/2017.
 */
public class LoanDataCleansingBolt extends BaseRichBolt {

    private OutputCollector collector;


    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    /**
     * Scrubs the raw loan data record received as a Tuple by either dropping the invalid records or reformatting the record based on the RegEx pattern
     * Emits the cleansed Tuple (Loan Record Key, Loan Record Value)
     *
     * @param loanRecordTuple
     */
    @Override
    public void execute(Tuple loanRecordTuple) {

        String loanRecord = loanRecordTuple.getString(2);

        // Drop the header record that defines the attribute names
        if (!(loanRecord.isEmpty() || loanRecord.contains("member_id") || loanRecord.contains("Total amount funded in policy code"))) {

            // Few records have emp_title with comma separated values resulting in records getting rejected.
            String scrubbedLoanRecord = loanRecord.replace(", ", "|").replaceAll("[a-z],", "");
            collector.emit(loanRecordTuple, new Values(loanRecordTuple.getString(0), scrubbedLoanRecord));
            collector.ack(loanRecordTuple);

        } else {

            System.out.println("Invalid Loan Record dropped at offset --> " + loanRecordTuple.getString(1) + " | " + loanRecordTuple.getString(2));
            collector.ack(loanRecordTuple);
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(LoanDataCleansingContants.LOAN_DATA_TUPLE_KEY, LoanDataCleansingContants.LOAN_DATA_SCRUBBED_TUPLE_LOANRECORD));
    }
}
