package es.david.flink.cap03;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;

public class Transformations06Aggregate {

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

        ///////////////////////////////
        DataSet<Tuple3<Long, Integer, Float>> agregacionResult = lineas
                .groupBy(0)                         // agrupamos por el campo 1
                .aggregate(Aggregations.SUM, 2)       // hacemos la suma por el campo 1
                .and(Aggregations.MIN, 1);

        agregacionResult.print();

        System.out.println("------");
        // si queremos añadir nuevas columnas podemos crear un nuevo dataset con las columnas añadidas y así ya podremos aplicar las funciones
        DataSet<Tuple4<Long, Integer, Float, Float>> pelisMx = pelis.map(new MapFunction<Tuple3<Long, Integer, Float>, Tuple4<Long, Integer, Float, Float>>() {
            @Override
            public Tuple4<Long, Integer, Float, Float> map(Tuple3<Long, Integer, Float> value) throws Exception {
                return new Tuple4(value.f0, value.f1, value.f2, 1F);
            }
        });
        pelisMx.print();

        System.out.println("------");
        pelisMx.groupBy(0)
                .aggregate(Aggregations.MAX, 1)
                .and(Aggregations.MAX, 2)
                .and(Aggregations.SUM, 3)
                .and(Aggregations.SUM, 2)
                .print();
        // para cada columna debe de haber solamente una funcion
    }
}
