import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.api.scala._

object Main extends App {

  case class Word(word: String, frecuency: Int)

  // Como el SparkContext
  val env = ExecutionEnvironment.getExecutionEnvironment

  // leemos el fichero y lo guardamos en el tipo de datos de Flink
  val lineas: DataSet[String] = env.readTextFile("/home/wizord/properties")

  lineas.flatMap { line => line.split(" ").map(word => Word(word, 1)) }
    .groupBy("word").sum("frecuency")
    .print()

}
