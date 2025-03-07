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

        val coAcquistiPart = coAcquisti.partitionBy(new HashPartitioner(executorsNumber.toInt * coreNumber.toInt * 3))

        // Sommare il numero di ordini in cui ogni coppia appare
        val coAcquistiCounts = coAcquistiPart
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

