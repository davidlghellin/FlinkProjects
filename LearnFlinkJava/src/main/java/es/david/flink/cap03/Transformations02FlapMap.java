package es.david.flink.cap03;

import es.david.flink.cap01.Pelicula;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.HashSet;

public class Transformations02FlapMap {

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

        System.out.printf("------");
        DataSet<String> generos = lineas.flatMap(new FlatMapFunction<Tuple3<Long, String, String>, String>() {
            @Override
            public void flatMap(Tuple3<Long, String, String> value, Collector<String> out) throws Exception {
                String generos[] = value.f2.split("\\|");
                for(String str:generos){
                    out.collect(str);
                }
            }
        });
        generos.print();
    }
}
