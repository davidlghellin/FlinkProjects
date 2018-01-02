package es.david.flink.cap03;

import es.david.flink.cap01.Pelicula;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.Arrays;
import java.util.HashSet;

public class Transformations03Filter {

    //https://ci.apache.org/projects/flink/flink-docs-release-1.3/dev/batch/dataset_transformations.html

    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = org.apache.flink.api.java.ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple3<Long, String, String>> lineas = env.readCsvFile("C:\\Users\\david.lgonzalez\\IdeaProjects\\JavaLearnFlink\\src\\main\\resources\\ratings\\movies.dat")
                // ignora la cabecera
                .ignoreFirstLine()
                // delimitador para el split
                .fieldDelimiter("::")
                // inserta los string con el caracter especificado
                .parseQuotedStrings('"')
                // ignora las lineas no correctas
                .ignoreInvalidLines()
                // soecificamos los tipos
                .types(Long.class, String.class, String.class);

        // El metodo map es una simple transformcion donde la entrada es un dataset y la salida es otro dataset
        DataSet<Pelicula> pelis = lineas.map(new MapFunction<Tuple3<Long, String, String>, Pelicula>() {
            @Override
            public Pelicula map(Tuple3<Long, String, String> csvLine) throws Exception {
                // nos quedmos con los nombres que se encuentran en el segundo elemento de la tupla
                String movieName = csvLine.f1;
                // nos quedmos con los nombres que se encuentran en el tercer elemento y le acemos un split
                String[] generos = csvLine.f2.split("\\|");
                // devolvemos un objeto
                return new Pelicula(movieName.toUpperCase(), new HashSet<>(Arrays.asList(generos)));
            }
        });

        // El metodo filter lo que hace es un filtrado por las condiciones que le digamos, devuelve un Dataset
        // Tendriamos que volver a hacer un map por ejemplo
        DataSet<Tuple3<Long, String, String>> filtrados = lineas.filter(new FilterFunction<Tuple3<Long, String, String>>() {
            @Override
            public boolean filter(Tuple3<Long, String, String> value) throws Exception {
                if (value.f1.contains("1995"))
                    return true;
                return false;
            }
        });
        filtrados.print();
    }
}
