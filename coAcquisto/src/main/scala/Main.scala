// import org.apache.spark.sql.{SparkSession, functions => F}
// import org.apache.spark.sql.types._
// import org.apache.spark.sql.functions._

// object CoAcquisto {

//   def main(args: Array[String]): Unit = {
//     // Creazione di SparkSession con configurazione per Kryo (serializzazione più efficiente)
//     val spark = SparkSession.builder()
//       .appName("CoAcquisto")
//     //   .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//       // Eventuale tuning delle partizioni per gli shuffle (valore esemplificativo)
//       //.config("spark.sql.shuffle.partitions", "200")
//       .getOrCreate()

//     import spark.implicits._

//     // Informazioni sulla macchina
//     val executorsNumber = spark.conf.get("spark.executor.instances", "1")
//     val coreNumber = Runtime.getRuntime.availableProcessors()
//     println("============================")
//     println(s"info macchina | instances: $executorsNumber, cores: $coreNumber |")
//     println("============================")

//     val bucketName = "co-purchase-bucket"
//     val filePath = s"gs://$bucketName/order_products.csv"
//     val outputPath = s"gs://$bucketName/outputs/output_${executorsNumber}"

//     // Misurazione del tempo d'inizio
//     val startTime = System.currentTimeMillis()

//     // Lettura del CSV utilizzando DataFrame (definendo lo schema per migliorare le prestazioni)
//     val schema = new StructType()
//       .add("order_id", IntegerType, nullable = false)
//       .add("product_id", IntegerType, nullable = false)

//     val ordersDF = spark.read
//       .option("header", "false")
//       .schema(schema)
//       .csv(filePath)
//       // Utilizza un numero di partizioni basato sul defaultParallelism per sfruttare al meglio le risorse
//       .repartition(spark.sparkContext.defaultParallelism)

//     // Raggruppa i prodotti per ordine usando collect_set per eliminare duplicati
//     val groupedDF = ordersDF.groupBy($"order_id")
//       .agg(F.collect_set($"product_id").alias("products"))

//     // UDF per generare tutte le coppie ordinate (combinazioni di due prodotti) da un insieme di prodotti
//     val generatePairs = F.udf { products: Seq[Int] =>
//       val sorted = products.sorted
//       // Genera tutte le combinazioni (coppie) con indice i < j
//       for {
//         i <- 0 until sorted.length
//         j <- i + 1 until sorted.length
//       } yield (sorted(i), sorted(j))
//     }

//     // Applica l'UDF per ottenere le coppie e "esplode" l'array in righe separate
//     val pairsDF = groupedDF
//       .withColumn("pairs", generatePairs($"products"))
//       .selectExpr("explode(pairs) as pair")
//       .select(
//         $"pair._1".alias("product1"),
//         $"pair._2".alias("product2")
//       )

//     // Raggruppa per coppia e conta il numero di co-acquisti
//     val coPurchaseCounts = pairsDF.groupBy($"product1", $"product2")
//       .agg(F.count("*").alias("count"))

//     println("============================")
//     println("CALCOLO EFFETTUATO")
//     println("============================")

//     // Scrive l'output in formato CSV. Se il risultato è contenuto, coalesce(1) riduce il numero di file in output.
//     coPurchaseCounts
//       .coalesce(1)
//       .write
//       .option("header", "false")
//       .csv(outputPath)

//     // Misura del tempo totale
//     val endTime = System.currentTimeMillis()
//     val elapsedTime = (endTime - startTime) / 1000.0
//     println("============================")
//     println(s"TEMPO TOTALE: $elapsedTime secondi")
//     println("============================")

//     spark.stop()
//   }
// }








import org.apache.spark.sql.{SparkSession, functions => F}
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.io.Source

import org.apache.spark.{SparkConf, SparkContext}
import java.nio.file.{Files, Paths, Path}
import scala.util.Try

import java.nio.file.{Files, Paths}
import scala.util.Try
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object CoAcquisto {


    def main(args: Array[String]): Unit = {
        //creazione spark context
        val conf = new SparkConf()
            .setAppName("CoAcquisto")
        val sc = new SparkContext(conf)

        val executorsNumber = sc.getConf.get("spark.executor.instances")
        val coreNumber = java.lang.Runtime.getRuntime.availableProcessors
        val partitionsNumber = executorsNumber.toInt * coreNumber.toInt * 2

        println("============================")
        println(s"info macchina |instances: $executorsNumber core: $coreNumber|")
        println("============================")

        val bucket_name = "co-purchase-bucket"

        // Percorso del file di input
        val filePath = "gs://" + bucket_name + "/order_products.csv"

        // Directory di output per il CSV
        val outputPath = "gs://" + bucket_name + "/outputs/output_" + executorsNumber


        // Misura il tempo di inizio
        val startTime = System.currentTimeMillis()

        // Lettura del file
        val rawData = sc.textFile(filePath)

        // Trasformare i dati in coppie (ordine, prodotto)
        val orderProductPairs = rawData
            .map(line => line.split(","))
            .map(parts => (parts(0).toInt, parts(1).toInt)) // (ordine, prodotto)
            .partitionBy(new HashPartitioner(partitionsNumber))

        // Raggruppare i prodotti per ordine
        val productsByOrder = orderProductPairs
            .groupByKey()

        // Generare tutte le combinazioni di co-acquisti per ogni ordine
        val coAcquisti = productsByOrder
            .flatMap { case (_, products) =>
                products.toSet.subsets(2).map(_.toList match {
                    case List(p1, p2) => ((p1, p2), 1)
                })
            }

        // Sommare il numero di ordini in cui ogni coppia appare
        val coAcquistiCounts = coAcquisti
          .partitionBy(new HashPartitioner(partitionsNumber))
          .reduceByKey(_ + _)   

        println("============================")
        println("CALCOLO EFFETTUATO")
        println("============================")

        // val coMax = coAcquistiCounts.max()(Ordering.by(_._2))
        // println("Max conf" + coMax)

        // Formattare il risultato come righe CSV (x, y, n)
        val csvOutput = coAcquistiCounts.map {
            case ((x, y), n) => s"$x,$y,$n"
        }

        // Salvare il risultato in un file CSV
        csvOutput.repartition(1).saveAsTextFile(outputPath)

        // Misura il tempo di fine
        val endTime = System.currentTimeMillis()
        val elapsedTime = (endTime - startTime) / 1000.0 // Tempo in secondi

        // Stampa il tempo totale
        println("============================")
        println(s"TEMPO TOTALE: $elapsedTime secondi")
        println("============================")

        // Ferma il contesto Spark
        sc.stop()
    }
}