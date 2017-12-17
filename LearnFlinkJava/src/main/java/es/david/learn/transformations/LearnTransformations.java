package es.david.learn.transformations;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;

public class LearnTransformations {

    public static void main(String[] args) throws Exception {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple3<String, String, String>> linesPelis =
                env.readCsvFile("/home/wizord/Git/FlinkProjects/LearnFlinkJava/src/main/resources/ratings/movies.dat")
                        // ignora la cabecera
                        .ignoreFirstLine()
                        .fieldDelimiter("::")
                        // inserta los string con el caracter especificado
                        .parseQuotedStrings('"')
                        // ignora las lineas no correctas
                        .ignoreInvalidLines()
                        // soecificamos los tipos
                        .types(String.class, String.class, String.class);

        linesPelis.print();
        // MAP
        // para cada uno de los elementos le aplica una funcion
        linesPelis.map(new MapFunction<Tuple3<String, String, String>, String>() {
            @Override
            public String map(Tuple3<String, String, String> value) throws Exception {
                return value.f1.toUpperCase();
            }
        }).print();
    }
}
