import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object CoAcquisto {


    def main(args: Array[String]): Unit = {
        // creazione spark context
        val conf = new SparkConf()
            .setAppName("CoAcquisto")
        val sc = new SparkContext(conf)

        // calcolo numero partitions
        val executorsNumber = sc.getConf.get("spark.executor.instances")
        val coreNumber = java.lang.Runtime.getRuntime.availableProcessors
        val partitionsNumber = executorsNumber.toInt * coreNumber.toInt * 2

        println("============================")
        println(s"info macchina |instances: $executorsNumber core: $coreNumber|")
        println("============================")

        // calcolo percorso dei file 
        val bucket_name = "co-purchase-bucket"
        val filePath = "gs://" + bucket_name + "/order_products.csv"                    // di input
        val outputPath = "gs://" + bucket_name + "/outputs/output_" + executorsNumber   // di output


        // Misura tempo di inizio
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
        val coAcquisti = productsByOrder.flatMap { case (_, products) =>
            products.toSet.subsets(2).map { subset =>
                val pair = subset.toList.sorted
                ((pair(0), pair(1)), 1)
            }
        }

        // Sommare il numero di ordini in cui ogni coppia appare
        val coAcquistiCounts = coAcquisti
          .partitionBy(new HashPartitioner(partitionsNumber))
          .reduceByKey(_ + _)   

        println("============================")
        println("CALCOLO EFFETTUATO")
        println("============================")

        // // Calcola il top 5 delle coppie di co-acquisti (in base al conteggio)
        // val top5 = coAcquistiCounts.top(5)(Ordering.by(_._2))
        // println("Top 5 co-acquisti:")
        // top5.foreach { case ((p1, p2), count) =>
        //     println(s"($p1, $p2): $count")
        // }

        // Formattare il risultato come righe CSV (x, y, n)
        val csvOutput = coAcquistiCounts.map {
            case ((x, y), n) => s"$x,$y,$n"
        }

        // Salvare il risultato in un file CSV
        csvOutput.repartition(1).saveAsTextFile(outputPath)

        // Misura tempo di fine
        val endTime = System.currentTimeMillis()
        val elapsedTime = (endTime - startTime) / 1000.0 // Tempo in secondi

        // Stampa del tempo totale
        println("============================")
        println(s"TEMPO TOTALE: $elapsedTime secondi")
        println("============================")

        sc.stop()
    }
}