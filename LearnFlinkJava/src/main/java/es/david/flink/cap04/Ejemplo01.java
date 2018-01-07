package es.david.flink.cap04;


import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;

public class Ejemplo01 {
    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);
        DataSet<WC> input = env.fromElements(new WC("Hello", 1), new WC("World",
                1), new WC("Hello", 1));

        // register the DataSet as table "WordCount"
        tEnv.registerDataSet("WordCount", input, "word, frequency");
        Table selectedTable = tEnv.sql("SELECT word, SUM(frequency) as frequency FROM WordCount GROUP BY word having word = 'Hello'");
        tEnv.registerTable("selected", selectedTable);

        DataSet<WC> result = tEnv.toDataSet(selectedTable, WC.class);

        result.print();
    }
}
