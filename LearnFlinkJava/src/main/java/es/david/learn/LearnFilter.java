package es.david.learn;

import es.david.learn.model.Pelicula;
import es.david.learn.model.Valoracion;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;

import java.util.Arrays;
import java.util.HashSet;

public class LearnFilter {
    public static void main(String[] args) throws Exception {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // filtros basicos
        DataSet<Integer> integers = env.fromElements(1, 2, 3, 4, 5, 6, 7);
        DataSet<Integer> filtered = integers.filter(new FilterFunction<Integer>() {
            @Override
            public boolean filter(Integer value) throws Exception {
                return value != 3;
            }
        });
        integers.print();
        System.out.println(integers.count());


        DataSet<Tuple3<String, String, String>> linesPelis =
                env.readCsvFile("/home/wizord/Git/FlinkProjects/LearnFlinkJava/src/main/resources/ratings/movies.dat")
                        // ignora la cabecera
                        .ignoreFirstLine()
                        // inserta los string con el caracter especificado
                        .parseQuotedStrings('"')
                        // ignora las lineas no correctas
                        .ignoreInvalidLines()
                        // soecificamos los tipos
                        .types(String.class, String.class, String.class);

        DataSet<Pelicula> pelis = linesPelis.map(new MapFunction<Tuple3<String, String, String>, Pelicula>() {
            @Override
            public Pelicula map(Tuple3<String, String, String> csvLine) throws Exception {
                String id = csvLine.f0;
                // nos quedmos con los nombres que se encuentran en el segundo elemento de la tupla
                String movieName = csvLine.f1;
                // nos quedmos con los nombres que se encuentran en el tercer elemento y le acemos un split
                String[] genres = csvLine.f2.split("\\|");
                // devolvemos un objeto
                return new Pelicula(id, movieName, new HashSet<>(Arrays.asList(genres)));
            }
        });

        DataSource<Tuple4<Integer, String, Integer, Long>> linesRatings =
                env.readCsvFile("/home/wizord/Git/FlinkProjects/LearnFlinkJava/src/main/resources/ratings/ratings.dat")
                        .ignoreFirstLine()
                        .parseQuotedStrings('"')
                        .ignoreInvalidLines()
                        .types(Integer.class, String.class, Integer.class, Long.class);

        DataSet<Valoracion> valoracion = linesRatings.map((new MapFunction<Tuple4<Integer, String, Integer, Long>, Valoracion>() {
            @Override
            public Valoracion map(Tuple4<Integer, String, Integer, Long> csvLine) throws Exception {
                Integer userId = csvLine.f0;
                String movieId = csvLine.f1;
                Integer rating = csvLine.f2;
                Long time = csvLine.f3;
                return new Valoracion(userId, movieId, rating, time);
            }
        }));

        /////////////////////////////////////////////////
    }
}
