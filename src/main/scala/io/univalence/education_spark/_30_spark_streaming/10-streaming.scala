package io.univalence.education_spark._30_spark_streaming

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{Serdes, StringDeserializer}
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import scala.jdk.CollectionConverters._
import scala.util.Using

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object _10_streaming {
  def main(args: Array[String]): Unit = {
    val conf =
      new SparkConf()
        .setAppName("StreamingApp")
        .setMaster("local[*]")
    val sc  = new SparkContext(conf)
    val ssc = new StreamingContext(sc, batchDuration = Seconds(5))

    ssc.checkpoint("data/output/checkpoint")

    val kafkaParams =
      Map[String, AnyRef](
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG        -> Configuration.bootstrapServers,
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG   -> classOf[StringDeserializer],
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
        ConsumerConfig.GROUP_ID_CONFIG                 -> "my_group",
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG        -> "latest",
        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG       -> (false: java.lang.Boolean)
      )

    val stream: InputDStream[ConsumerRecord[String, String]] =
      KafkaUtils.createDirectStream[String, String](
        ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe(List(Configuration.inputTopic), kafkaParams)
      )

    val updateFunc: (Seq[Int], Option[Int]) => Option[Int] =
      (ones, sum) => sum.map(sumValue => sumValue + ones.sum)

    val resultStream: DStream[(String, Int)] =
      stream
        .map(record => convertLine(record.value()))
        .map(order => order.product -> 1)
        .reduceByKey(_ + _)
        .updateStateByKey(updateFunc)

    resultStream.foreachRDD { (rdd: RDD[(String, Int)]) =>
      rdd.foreachPartition { partition =>
        Using(
          new KafkaProducer[String, String](
            Map[String, AnyRef](
              ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> Configuration.bootstrapServers
            ).asJava,
            Serdes.String().serializer(),
            Serdes.String().serializer()
          )
        ) { producer =>
          for ((product, count) <- partition) {
            val record = new ProducerRecord[String, String](Configuration.outputTopic, product, count.toString)
            producer.send(record).get()
          }
        }.get
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }

  def convertLine(line: String): Order = {
    val fields = line.split(",").toList

    Order(
      id       = fields(0),
      clientId = fields(1),
      timestamp =
        LocalDateTime.parse(
          fields(2),
          DateTimeFormatter.ISO_LOCAL_DATE_TIME
        ),
      product = fields(3),
      price   = fields(4).toDouble
    )
  }

  case class Order(
      id:        String,
      clientId:  String,
      timestamp: LocalDateTime,
      product:   String,
      price:     Double
  )

}
