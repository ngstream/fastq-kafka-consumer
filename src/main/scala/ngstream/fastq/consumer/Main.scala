package ngstream.fastq.consumer

import java.io.File
import java.time.Duration
import java.util.Properties

import ngstream.schema.{Fastq, FastqDataSerde, ReadIdSerde}
import org.apache.kafka.clients.consumer.KafkaConsumer
import scopt.{OParser, OParserBuilder}

import scala.collection.JavaConverters.asJavaCollection

object Main {
  def main(args: Array[String]): Unit = {
    val cmdArgs = OParser.parse(argParser, args, CmdArgs())
      .getOrElse(throw new IllegalArgumentException())

    val props = new Properties()
    props.put("bootstrap.servers", cmdArgs.brokers.mkString(","))
    props.put("key.deserializer", classOf[ReadIdSerde].getCanonicalName)
    props.put("value.deserializer", classOf[FastqDataSerde].getCanonicalName)
    props.put("group.id", "group2")

    val consumer = new KafkaConsumer[String, Fastq](props)

    consumer.subscribe(asJavaCollection(List(cmdArgs.topic)))

    while (true) {
      val bla = consumer.poll(Duration.ofSeconds(5))
      bla.iterator().forEachRemaining(x => println(x))
    }
  }

  case class CmdArgs(r1: File = null,
                     r2: Option[File] = None,
                     brokers: Seq[String] = Seq(),
                     topic: String = null)

  def argParser: OParser[Unit, CmdArgs] = {
    val builder: OParserBuilder[CmdArgs] = OParser.builder[CmdArgs]
    import builder._
    OParser.sequence(
      head("Ngstream fastq kafka producer"),
      opt[String]("broker")
        .action((x, c) => c.copy(brokers = c.brokers :+ x))
        .required()
        .text("Kafka broker, can supply multiple"),
      opt[String]("topic")
        .action((x, c) => c.copy(topic = x))
        .required()
        .text("Kafka topic")
    )
  }
}
