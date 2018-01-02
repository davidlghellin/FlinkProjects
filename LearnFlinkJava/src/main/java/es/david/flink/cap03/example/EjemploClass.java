package es.david.flink.cap03.example;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class EjemploClass {
    // https://ci.apache.org/projects/flink/flink-docs-release-1.3/dev/batch/iterations.html
    public static void main(String[] args) throws Exception {

        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Record> csvInput = env.readCsvFile("C:\\Users\\david.lgonzalez\\IdeaProjects\\JavaLearnFlink\\src\\main\\resources\\olympic-athletes.csv")
                .pojoType(Record.class, "playerName", "country", "year", "game", "gold", "silver", "bronze", "total");

        DataSet<Tuple2<String, Integer>> groupedByCountry = csvInput
                .flatMap(new FlatMapFunction<Record, Tuple2<String, Integer>>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public void flatMap(Record record, Collector<Tuple2<String, Integer>> out) throws Exception {

                        out.collect(new Tuple2<String, Integer>(record.getCountry(), 1));
                    }
                }).groupBy(0).sum(1);
        groupedByCountry.print();

        DataSet<Tuple2<String, Integer>> groupedByGame = csvInput
                .flatMap(new FlatMapFunction<Record, Tuple2<String, Integer>>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public void flatMap(Record record, Collector<Tuple2<String, Integer>> out) throws Exception {

                        out.collect(new Tuple2<String, Integer>(record.getGame(), 1));
                    }
                }).groupBy(0).sum(1);
        groupedByGame.print();

    }

}
