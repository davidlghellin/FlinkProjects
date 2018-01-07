package es.david.flink.cap04;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;

import java.util.List;

/**
 * Simple example for demonstrating the use of the Table API for a Word Count in Java.
 * <p>
 * <p>This example shows how to:
 * - Convert DataSets to Tables
 * - Apply group, aggregate, select, and filter operations
 */
public class WordCountTable {

    // *************************************************************************
    //     PROGRAM
    // *************************************************************************

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.createCollectionsEnvironment();
        BatchTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);

        DataSet<WC> input = env.fromElements(
                new WC("Hello", 1),
                new WC("Ciao", 1),
                new WC("Hello", 1));

        System.out.println(input.count());

        Table table = tEnv.fromDataSet(input);
        System.out.println(table.groupBy("word"));

        System.out.println("-------1");

        table.groupBy("word").table().printSchema();

        System.out.println("-------2");

        Table grop = table.groupBy("word").table();
        System.out.println("-------3");

        System.out.println(grop.tableName());

        System.out.println("-------4");

        DataSet<WC> preresult = tEnv.toDataSet(table, WC.class);

        List<WC> lista=preresult.collect();
        System.out.println("lista = " + lista.size());

        preresult.print();
        System.out.println("-------5");


        Table filtered = table
                .groupBy("word")
                .select("word, frequency.sum as frequency")
                .filter("frequency = 2");

        DataSet<WC> result = tEnv.toDataSet(filtered, WC.class);

        result.print();
    }

    // *************************************************************************
    //     USER DATA TYPES
    // *************************************************************************

    /**
     * Simple POJO containing a word and its respective count.
     */
    public static class WC {
        public String word;
        public long frequency;

        // public constructor to make it a Flink POJO
        public WC() {
        }

        public WC(String word, long frequency) {
            this.word = word;
            this.frequency = frequency;
        }

        @Override
        public String toString() {
            return "WC " + word + " " + frequency;
        }
    }
}