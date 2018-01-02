package es.david.flink.cap03;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;

public class Transformations08ReduceFull {

    //https://ci.apache.org/projects/flink/flink-docs-release-1.3/dev/batch/dataset_transformations.html

    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = org.apache.flink.api.java.ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple3<Long, Integer, Float>> lineas = env.readCsvFile("C:\\Users\\david.lgonzalez\\IdeaProjects\\JavaLearnFlink\\src\\main\\resources\\ratings\\compras.dat")
                // ignora la cabecera
                .ignoreFirstLine()
                // delimitador para el split
                .fieldDelimiter("::")
                // inserta los string con el caracter especificado
                .parseQuotedStrings('"')
                // ignora las lineas no correctas
                .ignoreInvalidLines()
                // soecificamos los tipos
                .types(Long.class, Integer.class, Float.class);

        // El metodo map es una simple transformcion donde la entrada es un dataset y la salida es otro dataset
        DataSet<Tuple3<Long, Integer, Float>> pelis = lineas.map(new MapFunction<Tuple3<Long, Integer, Float>, Tuple3<Long, Integer, Float>>() {
            @Override
            public Tuple3<Long, Integer, Float> map(Tuple3<Long, Integer, Float> csvLine) throws Exception {
                Long id = csvLine.f0;
                int idArticulo = csvLine.f1;
                float precio = csvLine.f2;

                // devolvemos un objeto
                return new Tuple3<Long, Integer, Float>(id, idArticulo, precio);
            }
        });
        // Esta es la forma de aplicar nuestras funciones personalizadas para el reducer
        class IntSumReducer implements ReduceFunction<Integer> {
            @Override
            public Integer reduce(Integer num1, Integer num2) {
                return num1 + num2;
            }
        }
        // En este caso sumaremos los valores de los grupos e iremos propagando el valor del primero
        class SumReducerTuple implements ReduceFunction<Tuple3<Long, Integer, Float>> {
            @Override
            public Tuple3<Long, Integer, Float> reduce(Tuple3<Long, Integer, Float> value, Tuple3<Long, Integer, Float> t1) throws Exception {
                return new Tuple3<>(value.f0 + t1.f0, value.f1, value.f2);
            }
        }
        lineas.reduce(new SumReducerTuple()).print();

    }
}
