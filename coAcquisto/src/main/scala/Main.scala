import org.apache.spark.sql.{SparkSession, functions => F}
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.io.Source

// object WordCount {
//   def main(args: Array[String]): Unit = {
//     // Creazione della sessione Spark
//     val spark = SparkSession.builder()
//       .appName("Name Count Example")
//       .master("local[*]") // Usa tutte le CPU disponibili
//       .getOrCreate()

//     // Leggi un file CSV con una colonna "name"
//     val filePath = "./src/names.csv" // Sostituisci con il tuo percorso
//     val data = spark.read.option("header", "true").csv(filePath)

//     // Conta le occorrenze di ogni nome
//     val nameCounts = data.groupBy("name")
//       .agg(F.count("name").as("count"))
//       .orderBy(F.desc("count"))

//     // Mostra i risultati
//     nameCounts.show()

//     // Chiudi la sessione Spark
//     spark.stop()
//   }
// }

import org.apache.spark.{SparkConf, SparkContext}
import java.nio.file.{Files, Paths, Path}
import scala.util.Try

import java.nio.file.{Files, Paths}
import scala.util.Try
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object CoAcquisto {
    // Metodo per cancellare la directory di output se esiste
    def deleteDirectory(directory: String): Unit = {
        val path = Paths.get(directory)
        if (Files.exists(path)) {
            Try {
                Files.walk(path)
                    .sorted(java.util.Comparator.reverseOrder())
                    .forEach(Files.delete)
            }.recover {
                case e: Exception =>
                    println(s"Errore durante la cancellazione della directory $directory: ${e.getMessage}")
            }
        }
    }

    def main(args: Array[String]): Unit = {
        val filePath = "gs://bucket_test_fusillo/order_products.csv"
        // val filePath = "./src/order_products.csv" // Percorso del file di input // ===== DATAPROC =====

        val outputPath = "gs://bucket_test_fusillo/output" // Directory di output per il CSV
        // val outputPath = "./output" // Directory di output per il CSV // ===== DATAPROC =====

        // Cancella la directory di output se già esiste
        deleteDirectory(outputPath)

        // Configurazione Spark per utilizzare tutti i core disponibili
        val conf = new SparkConf()
            .setAppName("CoAcquisto")
            // .setMaster("local[*]") // Utilizza tutti i core disponibili // ===== DATAPROC =====
        val sc = new SparkContext(conf)

        // Misura il tempo di inizio
        val startTime = System.currentTimeMillis()

        // Lettura del file
        val rawData = sc.textFile(filePath)

        // Trasformare i dati in coppie (ordine, prodotto)
        val orderProductPairs = rawData
            .map(line => line.split(","))
            .map(parts => (parts(0).toInt, parts(1).toInt)) // (ordine, prodotto)

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

        val coAcquistiPart = coAcquisti.partitionBy(new HashPartitioner(200))

        // Sommare il numero di ordini in cui ogni coppia appare
        val coAcquistiCounts = coAcquistiPart
            .reduceByKey(_ + _)    

        val coMax = coAcquistiCounts.max()(Ordering.by(_._2))
        println("Max conf" + coMax)

        // Formattare il risultato come righe CSV (x, y, n)
        val csvOutput = coAcquistiCounts.map {
            case ((x, y), n) => s"$x,$y,$n"
        }

        // Salvare il risultato in un file CSV
        csvOutput.saveAsTextFile(outputPath)

        // Misura il tempo di fine
        val endTime = System.currentTimeMillis()
        val elapsedTime = (endTime - startTime) / 1000.0 // Tempo in secondi

        // Stampa il tempo totale
        println(s"Tempo totale di esecuzione: $elapsedTime secondi")

        // Ferma il contesto Spark
        sc.stop()
    }
}

