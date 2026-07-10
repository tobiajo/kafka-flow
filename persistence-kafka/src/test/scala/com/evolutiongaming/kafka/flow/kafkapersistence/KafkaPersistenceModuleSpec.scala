package com.evolutiongaming.kafka.flow.kafkapersistence

import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Ref, Resource}
import cats.syntax.all.*
import com.evolutiongaming.catshelper.LogOf
import com.evolutiongaming.kafka.flow.PartitionAssignment
import com.evolutiongaming.skafka.consumer.{Consumer as SkafkaConsumer, ConsumerConfig, ConsumerGroupMetadata, ConsumerOf}
import com.evolutiongaming.skafka.producer.{Producer, ProducerConfig, ProducerOf}
import com.evolutiongaming.skafka.{CommonConfig, FromBytes, Offset, Partition, TopicPartition}
import munit.FunSuite

import scala.concurrent.duration.*

/** The transactional module owns the producer settings that carry its design: the stable per-partition
  * `transactional.id` (a takeover must abort a crashed owner's dangling transaction - the recovery read's bound relies
  * on it), idempotence, and the 10 s `transaction.timeout.ms` default (the Kafka Streams EOS default) - all applied
  * over whatever `producerConfig` carries.
  */
class KafkaPersistenceModuleSpec extends FunSuite {

  implicit val logOf: LogOf[IO] = LogOf.empty[IO]

  private val inputPartition = TopicPartition("input-topic", Partition.min)

  private def acquireModule(config: KafkaPersistenceModule.TransactionalConfig): IO[Option[ProducerConfig]] =
    for {
      captured <- Ref.of[IO, Option[ProducerConfig]](none)
      producerOf = new ProducerOf[IO] {
        def apply(config: ProducerConfig): Resource[IO, Producer[IO]] =
          Resource.eval(captured.set(config.some)).as(Producer.empty[IO])
      }
      // recovery reads lazily (keysOf.all); module acquisition itself must not open a consumer
      consumerOf = new ConsumerOf[IO] {
        def apply[K, V](
          config: ConsumerConfig
        )(implicit fromBytesK: FromBytes[IO, K], fromBytesV: FromBytes[IO, V]) =
          Resource.eval(IO.raiseError[SkafkaConsumer[IO, K, V]](new IllegalStateException("consumer opened at acquisition")))
      }
      moduleOf = KafkaPersistenceModuleOf.cachingTransactional[IO, String](
        consumerOf = consumerOf,
        producerOf = producerOf,
        config     = config,
      )
      groupMetadata = IO.pure(none[ConsumerGroupMetadata])
      _            <- moduleOf.make(PartitionAssignment(inputPartition, Offset.min, groupMetadata)).use_
      config       <- captured.get
    } yield config

  private def transactionalConfig = KafkaPersistenceModule.TransactionalConfig(
    consumerConfig        = ConsumerConfig(),
    producerConfig        = ProducerConfig(common = CommonConfig(clientId = "client".some)),
    transactionalIdPrefix = "app",
    snapshotTopic         = "state-topic",
  )

  test("the module applies the stable per-partition id, idempotence and the 10 s timeout default") {
    val test = acquireModule(transactionalConfig).map { config =>
      val produced = config.getOrElse(fail("no producer was created at module acquisition"))
      assertEquals(produced.transactionalId, "app-0".some)
      assertEquals(produced.idempotence, true)
      assertEquals(produced.transactionTimeout, 10.seconds)
      assertEquals(produced.common.clientId, "client-snapshot-0".some)
    }
    test.unsafeRunSync()
  }

  test("a configured transactionTimeout overrides the default (producerConfig's own value never applies)") {
    val test = acquireModule(
      transactionalConfig.copy(
        producerConfig     = ProducerConfig(transactionTimeout = 1.minute),
        transactionTimeout = 3.seconds,
      )
    ).map { config =>
      val produced = config.getOrElse(fail("no producer was created at module acquisition"))
      assertEquals(produced.transactionTimeout, 3.seconds)
    }
    test.unsafeRunSync()
  }
}
