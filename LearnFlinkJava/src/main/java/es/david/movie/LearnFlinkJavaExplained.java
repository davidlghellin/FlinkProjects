package es.david.movie;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.Arrays;
import java.util.HashSet;

public class LearnFlinkJavaExplained {

    public static void main(String[] args) throws Exception {

        final ExecutionEnvironment env = org.apache.flink.api.java.ExecutionEnvironment.getExecutionEnvironment();

        // Load dataset of movies
        DataSet<Tuple3<Long, String, String>> lines = env.readCsvFile("/home/wizord/Git/FlinkProjects/LearnFlinkJava/src/main/resources/movies.csv")
                // ignora la cabecera
                .ignoreFirstLine()
                // inserta los string con el caracter especificado
                .parseQuotedStrings('"')
                // ignora las lineas no correctas
                .ignoreInvalidLines()
                // soecificamos los tipos
                .types(Long.class, String.class, String.class);

        DataSet<Pelicula> pelis = lines.map(new MapFunction<Tuple3<Long, String, String>, Pelicula>() {
            @Override
            public Pelicula map(Tuple3<Long, String, String> csvLine) throws Exception {
                // nos quedmos con los nombres que se encuentran en el segundo elemento de la tupla
                String movieName = csvLine.f1;
                // nos quedmos con los nombres que se encuentran en el tercer elemento y le acemos un split
                String[] genres = csvLine.f2.split("\\|");
                // devolvemos un objeto
                return new Pelicula(movieName, new HashSet<>(Arrays.asList(genres)));
            }
        });

        // filtraemos las peliculas
        DataSet<Pelicula> filteredMovies = pelis.filter(new FilterFunction<Pelicula>() {
            @Override
            public boolean filter(Pelicula movie) throws Exception {
                return movie.getGeneros().contains("Action");
            }
        });

        pelis.writeAsText("/home/wizord/output.txt");

        // Start Flink
        env.execute();
    }
}
