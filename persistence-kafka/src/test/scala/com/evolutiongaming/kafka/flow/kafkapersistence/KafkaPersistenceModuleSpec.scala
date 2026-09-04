package com.evolutiongaming.kafka.flow.kafkapersistence

import cats.Applicative
import cats.data.NonEmptyMap
import cats.effect.testkit.TestControl
import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Ref, Resource}
import cats.syntax.all.*
import com.evolutiongaming.catshelper.LogOf
import com.evolutiongaming.kafka.flow.kafka.GenerationFencedError
import com.evolutiongaming.kafka.flow.snapshot.SnapshotWriteMetrics
import com.evolutiongaming.kafka.flow.{FlowMetrics, PartitionAssignment}
import com.evolutiongaming.skafka.OffsetAndMetadata
import org.apache.kafka.clients.consumer.CommitFailedException
import com.evolutiongaming.skafka.consumer.{
  AutoOffsetReset,
  Consumer as SkafkaConsumer,
  ConsumerConfig,
  ConsumerGroupMetadata,
  ConsumerOf,
  IsolationLevel
}
import com.evolutiongaming.skafka.producer.{Producer, ProducerConfig, ProducerOf}
import com.evolutiongaming.skafka.{CommonConfig, FromBytes, Offset, Partition, TopicPartition}
import munit.FunSuite

import scala.concurrent.duration.*

/** The transactional module owns the producer settings its design depends on: the stable per-partition
  * `transactional.id` (a takeover must abort a crashed owner's unfinished transaction) and idempotence - applied over
  * whatever `producerConfig` carries. Its recovery read is wired `read_committed` from earliest with the configured
  * deadline enabled, and its ephemeral consumers are group-less and never commit offsets - a committed offset would
  * override the earliest reset on the next recovery. Its writer gets the configured fence tolerance and the
  * `FlowMetrics` fence counter.
  */
class KafkaPersistenceModuleSpec extends FunSuite {

  implicit val logOf: LogOf[IO] = LogOf.empty[IO]

  // recovery reads lazily (keysOf.all); module acquisition itself must not open a consumer
  private def unusedConsumerOf: ConsumerOf[IO] = new ConsumerOf[IO] {
    def apply[K, V](
      config: ConsumerConfig
    )(implicit fromBytesK: FromBytes[IO, K], fromBytesV: FromBytes[IO, V]) =
      Resource.eval(
        IO.raiseError[SkafkaConsumer[IO, K, V]](new IllegalStateException("consumer opened at acquisition"))
      )
  }

  test("the module applies the stable per-partition id, idempotence and the suffixed client id") {
    val test = for {
      captured <- Ref.of[IO, Option[ProducerConfig]](none)
      producerOf = new ProducerOf[IO] {
        def apply(config: ProducerConfig): Resource[IO, Producer[IO]] =
          Resource.eval(captured.set(config.some)).as(Producer.empty[IO])
      }
      config = KafkaPersistenceModule.TransactionalConfig(
        consumerConfig        = ConsumerConfig(),
        producerConfig        = ProducerConfig(common = CommonConfig(clientId = "client".some)),
        transactionalIdPrefix = "app",
        snapshotTopic         = "state-topic",
      )
      assignment = PartitionAssignment[IO](
        topicPartition = TopicPartition("input-topic", Partition.min),
        assignedAt     = Offset.min,
        groupMetadata  = IO.pure(none[ConsumerGroupMetadata]),
      )
      _ <- KafkaPersistenceModule
        .cachingTransactional[IO, String](unusedConsumerOf, producerOf, config, assignment)
        .use_
      config <- captured.get
    } yield {
      val produced = config.getOrElse(fail("no producer was created at module acquisition"))
      assertEquals(produced.transactionalId, "app-snapshot-0".some)
      assertEquals(produced.idempotence, true)
      assertEquals(produced.common.clientId, "client-snapshot-0".some)
    }
    test.unsafeRunSync()
  }

  test("the module's recovery read is read_committed from earliest, suffixed, offsets-neutral, and deadline-enabled") {
    // a parked recovery driven through keysOf.all: the captured configs and the stall error pin the wiring
    val tp    = TopicPartition("state-topic", Partition.min)
    val fakes = new FakeConsumers(tp)
    val test = for {
      captured    <- Ref.of[IO, List[ConsumerConfig]](Nil)
      positionRef <- Ref.of[IO, Long](0L)
      readConsumer = fakes.consumer(endOffset = 1L, positionRef = positionRef, records = Nil)
      hwConsumer   = fakes.consumer(endOffset = 3L, positionRef = positionRef, records = Nil)
      inner        = fakes.consumerOf(readConsumer = readConsumer, hwConsumer = hwConsumer)
      capturingOf = new ConsumerOf[IO] {
        def apply[K, V](
          config: ConsumerConfig
        )(implicit fromBytesK: FromBytes[IO, K], fromBytesV: FromBytes[IO, V]) =
          Resource.eval(captured.update(_ :+ config)) *> inner(config)
      }
      producerOf = new ProducerOf[IO] {
        def apply(config: ProducerConfig): Resource[IO, Producer[IO]] = Resource.pure(Producer.empty[IO])
      }
      result <- KafkaPersistenceModule
        .cachingTransactional[IO, String](
          consumerOf = capturingOf,
          producerOf = producerOf,
          config = KafkaPersistenceModule.TransactionalConfig(
            // the hazardous shape: a group plus auto-commit, which the module must clear on its ephemeral
            // readers - a committed offset would override the earliest reset on the next recovery
            consumerConfig = ConsumerConfig(
              common     = CommonConfig(clientId = "client".some),
              groupId    = "app-group".some,
              autoCommit = true,
            ),
            producerConfig        = ProducerConfig(),
            transactionalIdPrefix = "app",
            snapshotTopic         = "state-topic",
            recoveryStallTimeout  = 200.millis,
          ),
          assignment = PartitionAssignment[IO](
            topicPartition = TopicPartition("input-topic", Partition.min),
            assignedAt     = Offset.min,
            groupMetadata  = IO.pure(none[ConsumerGroupMetadata]),
          ),
        )
        .use(_.keysOf.all("app", "group", tp).toList.timeout(1.minute))
        .attempt
      configs <- captured.get
    } yield {
      result match {
        case Left(_: KafkaPartitionPersistence.RecoveryReadStalledError) => ()
        case other => fail(s"expected the enabled deadline to fail the parked recovery, got $other")
      }
      val read =
        configs.find(_.common.clientId.contains("client-snapshot-0")).getOrElse(fail(s"no read consumer: $configs"))
      val hw =
        configs.find(_.common.clientId.contains("client-snapshot-0-hw")).getOrElse(fail(s"no hw consumer: $configs"))
      assertEquals(read.isolationLevel, IsolationLevel.ReadCommitted)
      assertEquals(read.autoOffsetReset, AutoOffsetReset.Earliest)
      assertEquals(hw.isolationLevel, IsolationLevel.ReadUncommitted)
      List(read, hw).foreach { config =>
        assertEquals(config.groupId, none[String])
        assertEquals(config.autoCommit, false)
      }
    }
    TestControl.executeEmbed(test).unsafeRunSync()
  }

  List(
    "the default tolerance surfaces the fence as GenerationFencedError" -> (none[FiniteDuration], true),
    "fenceTolerance = Zero fails on the first fence"                    -> (Duration.Zero.some, false),
  ).foreach {
    case (name, (fenceTolerance, tolerated)) =>
      test(s"the module wires fenceTolerance and the fence counter into its writer: $name") {
        // the broker's rejection of the offset commit, as kafka-clients raises it
        val fencingProducer: Producer[IO] = new Producer[IO] {
          private val base                = Producer.empty[IO]
          def initTransactions: IO[Unit]  = base.initTransactions
          def beginTransaction: IO[Unit]  = base.beginTransaction
          def commitTransaction: IO[Unit] = base.commitTransaction
          def abortTransaction: IO[Unit]  = base.abortTransaction
          def sendOffsetsToTransaction(
            offsets: NonEmptyMap[TopicPartition, OffsetAndMetadata],
            consumerGroupMetadata: ConsumerGroupMetadata,
          ): IO[Unit] = IO.raiseError(new CommitFailedException("stale generation"))
          def send[K, V](record: com.evolutiongaming.skafka.producer.ProducerRecord[K, V])(
            implicit toBytesK: com.evolutiongaming.skafka.ToBytes[IO, K],
            toBytesV: com.evolutiongaming.skafka.ToBytes[IO, V]
          ) = base.send(record)
          def partitions(topic: com.evolutiongaming.skafka.Topic) = base.partitions(topic)
          def flush: IO[Unit]                                     = base.flush
          def clientMetrics                                       = base.clientMetrics
          def clientInstanceId(timeout: FiniteDuration)           = base.clientInstanceId(timeout)
        }
        val inputTopicPartition = TopicPartition("input-topic", Partition.min)
        val test = for {
          fences <- Ref.of[IO, List[TopicPartition]](Nil)
          metrics = new FlowMetrics[IO] {
            private val empty                        = FlowMetrics.empty[IO]
            def keyDatabaseMetrics                   = empty.keyDatabaseMetrics
            def journalDatabaseMetrics               = empty.journalDatabaseMetrics
            def snapshotDatabaseMetrics              = empty.snapshotDatabaseMetrics
            def persistenceModuleMetrics             = empty.persistenceModuleMetrics
            def foldOptionMetrics                    = empty.foldOptionMetrics
            def enhancedFoldMetrics                  = empty.enhancedFoldMetrics
            def keyStateOfMetrics                    = empty.keyStateOfMetrics
            def partitionFlowOfMetrics               = empty.partitionFlowOfMetrics
            def topicFlowOfMetrics                   = empty.topicFlowOfMetrics
            def compressorMetrics(component: String) = empty.compressorMetrics(component)
            def snapshotWriteMetrics = new SnapshotWriteMetrics[IO] {
              def fenced(topicPartition: TopicPartition)(implicit F: Applicative[IO]): IO[Unit] =
                fences.update(_ :+ topicPartition)
            }
          }
          base = KafkaPersistenceModule.TransactionalConfig(
            consumerConfig        = ConsumerConfig(),
            producerConfig        = ProducerConfig(),
            transactionalIdPrefix = "app",
            snapshotTopic         = "state-topic",
          )
          config = fenceTolerance.fold(base)(tolerance => base.copy(fenceTolerance = tolerance))
          producerOf = new ProducerOf[IO] {
            def apply(config: ProducerConfig): Resource[IO, Producer[IO]] = Resource.pure(fencingProducer)
          }
          assignment = PartitionAssignment[IO](
            topicPartition = inputTopicPartition,
            assignedAt     = Offset.min,
            groupMetadata  = IO.pure(ConsumerGroupMetadata.Empty.some),
          )
          result <- KafkaPersistenceModule
            .cachingTransactional[IO, String](unusedConsumerOf, producerOf, config, assignment, metrics)
            .use { module =>
              module
                .scheduleCommit
                .getOrElse(fail("transactional module exposes no ScheduleCommit"))
                .schedule(Offset.min)
            }
            .attempt
          fenced <- fences.get
        } yield {
          result match {
            case Left(_: GenerationFencedError) => assert(tolerated, s"expected the raw fence, got $result")
            case Left(_: CommitFailedException) => assert(!tolerated, s"expected GenerationFencedError, got $result")
            case other                          => fail(s"unexpected outcome $other")
          }
          assertEquals(fenced, List(inputTopicPartition))
        }
        test.unsafeRunSync()
      }
  }

}
