package es.david.learn;

import es.david.learn.model.Pelicula;
import es.david.learn.model.Valoracion;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;

import java.util.Arrays;
import java.util.HashSet;

public class LearnOptimize {
    public static void main(String[] args) throws Exception {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple3<String, String, String>> linesPelis =
                env.readCsvFile("/home/wizord/Git/FlinkProjects/LearnFlinkJava/src/main/resources/ratings/movies.dat")
                        // ignora la cabecera
                        .ignoreFirstLine()
                        // inserta los string con el caracter especificado
                        .parseQuotedStrings('"')

                        .fieldDelimiter("::")
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
                        .fieldDelimiter("::")
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


        //  mostramos 10
        linesPelis.first(10).print();
        // ordenamos y mostramos 10
        linesPelis.sortPartition(0, Order.DESCENDING).first(10).print();


        /////////////////////////////////////////////////
        // OPTIMIZACIONES
        /////////////////////////////////////////////////
        // como podemos ver en la pagina https://brewing.codes/2017/10/17/flink-optimize/
      /*  DataSet<Pelicula> join =
                pelis.join(valoracion)

                .where(new KeySelector<Pelicula, String>() {
                    @Override
                    public String getKey(Pelicula pelicula) throws Exception {
                        return pelicula.getId();
                    }
                })
                .equalTo(new KeySelector<Valoracion, String>() {
                    @Override
                    public String getKey(Valoracion r) throws Exception {
                        return r.getPeliId();
                    }
                });


        System.out.println("Tenemos: " + join.count());
        System.out.println("Generico1: " + join.getInput1().getType());
        System.out.println("Generico2: " + join.getInput2().getType());

*/
        //   System.out.println(linesPelis.count());


        System.out.println("---");
        DataSet<Tuple3<Integer, String, String>> joinDocsRanks =
                linesRatings.join(linesPelis)
                        .where(1)
                        .equalTo(0)
                        .projectSecond(0, 1, 2);
        joinDocsRanks.print();
        System.out.println("---");
        System.out.println(joinDocsRanks.count());
        System.out.println(joinDocsRanks.collect().size());
        System.out.println("---");
        // Filter

     /*   DataSet<Tuple1<String>> filterVisits = linesRatings
                .filter(new FilterRating4())
                .project(1);

        filterVisits.print();

        System.out.println(linesPelis.count());
        System.out.println(linesRatings.count() + " linesRating");
        System.out.println(filterVisits.count() + " cuanta");


*/

    }

    public static class FilterRating4 implements FilterFunction<Tuple4<Integer, String, Integer, Long>> {
        @Override
        public boolean filter(Tuple4<Integer, String, Integer, Long> value) throws Exception {
            int valoracion = value.f2;
            return valoracion == 4;
        }
    }
}