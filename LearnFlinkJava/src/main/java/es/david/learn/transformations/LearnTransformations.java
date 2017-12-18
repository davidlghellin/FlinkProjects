package es.david.learn.transformations;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

public class LearnTransformations {

    //https://ci.apache.org/projects/flink/flink-docs-release-1.3/api/java/
    //https://www.programcreek.com/java-api-examples/index.php?api=org.apache.flink.api.common.functions.FlatMapFunction

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
        System.out.printf("------");


        // FLAT MAP
        // Como el map, toma un elemento pero devuelve cero, uno o más elementos
        linesPelis.flatMap(new FlatMapFunction<Tuple3<String, String, String>, Object>() {

            @Override
            public void flatMap(Tuple3<String, String, String> value, Collector<Object> out) throws Exception {
                String generos[] = value.f2.split("\\|");
                for (String str : generos) {
                    out.collect(str);
                }
            }
        }).print();
        System.out.printf("------");

        DataSet<Tuple3<String, String, String>> flatmap = linesPelis.flatMap(new FlatMapFunction<Tuple3<String, String, String>, Tuple3<String, String, String>>() {

            @Override
            public void flatMap(Tuple3<String, String, String> value, Collector<Tuple3<String, String, String>> out) throws Exception {
                String generos[] = value.f2.split("\\|");
                for (String str : generos) {
                    out.collect(new Tuple3<String, String, String>(value.f0, value.f1, str));
                }
            }
        });
        flatmap.print();
        System.out.printf("------");

        // FILTER
        // La funcion filter, evalua la condicion y en caso de devolver un true lo añade
        // Puede devolver 0 elementos
        DataSet<Tuple3<String, String, String>> filtrados = linesPelis.filter(new FilterFunction<Tuple3<String, String, String>>() {
            @Override
            public boolean filter(Tuple3<String, String, String> value) throws Exception {
                if (value.f1.contains("1995"))
                    return true;
                return false;
            }
        });
        filtrados.print();
        System.out.printf("------");

        // PROJECT
        // Las transformaciones project, lo que hacen es mover o eliminar los elementos de una tupla a otra
        // Puede ser usado para procesamiento selectivo
        DataSet<Tuple2<String, String>> project = linesPelis.project(1, 0);
        project.print();
        System.out.printf("------");

        // REDUCER
        // agrupa los elementos de cada grupo por medio de la funcion definida por el usuario
        DataSet<Tuple3<String, String, String>> reducePelis = linesPelis.reduce(new ReduceFunction<Tuple3<String, String, String>>() {
            @Override
            public Tuple3<String, String, String> reduce(Tuple3<String, String, String> value, Tuple3<String, String, String> out) throws Exception {
                return new Tuple3<String, String, String>(value.f0, out.f1 + ";\t" + value.f1, "");
            }
        });
        reducePelis.print();
        System.out.printf("------");

        // tambien podemos agrupar dataset por campos o por posicion
        // en este caso es mas dificil ya que solamante tenemos string
        linesPelis.groupBy(0).reduce(new ReduceFunction<Tuple3<String, String, String>>() {
            @Override
            public Tuple3<String, String, String> reduce(Tuple3<String, String, String> value, Tuple3<String, String, String> out) throws Exception {
                // TODO
                return null;
            }
        });

        // GROUP COMBINE
        // Algunas veces es necesario hacer operaciones intermedias antes de hacer mas transformaciones
        // Las agrupaciones de combinacion pueden ser utiles en estos casos
        // Esto se realiza en memoria
        linesPelis
                .groupBy(0)
                .combineGroup(new GroupCombineFunction<Tuple3<String, String, String>, Tuple2<String, Integer>>() {
                    @Override
                    public void combine(Iterable<Tuple3<String, String, String>> iterable, Collector<Tuple2<String, Integer>> out) throws Exception {
                        int nVocalA = 0;
                        // TODO revisar con algun ejemplo mejor
                    }
                });


        DataSet<Tuple3<Integer, String, String>> linesPelisInt =
                env.readCsvFile("/home/wizord/Git/FlinkProjects/LearnFlinkJava/src/main/resources/ratings/movies.dat")
                        // ignora la cabecera
                        .ignoreFirstLine()
                        .fieldDelimiter("::")
                        // inserta los string con el caracter especificado
                        .parseQuotedStrings('"')
                        // ignora las lineas no correctas
                        .ignoreInvalidLines()
                        // soecificamos los tipos
                        .types(Integer.class, String.class, String.class);

        // AGREGACION
        // Las transformaciones de agregacion son muy comunes, se puede facilmente
        // Podemos hacer agregaciones facilmente como sum, min, max

        DataSet<Tuple3<Integer, String, String>> agregacionResult = linesPelisInt
                .groupBy(0)                     // agrupamos por el campo 0
                .aggregate(Aggregations.SUM, 0)     // hacemos la suma por el campo 1 (es string)
                .and(Aggregations.MIN, 0);

        // agregacionResult.print();
        System.out.printf("------");


        // MinBy
        // MaxBy
    }
}
