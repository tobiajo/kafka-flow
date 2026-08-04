package com.evolutiongaming.kafka.flow.kafkapersistence

import cats.effect.testkit.TestControl
import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Ref, Resource}
import cats.syntax.all.*
import com.evolutiongaming.catshelper.{Log, LogOf}
import com.evolutiongaming.kafka.flow.PartitionAssignment
import com.evolutiongaming.skafka.consumer.{
  AutoOffsetReset,
  Consumer as SkafkaConsumer,
  ConsumerConfig,
  ConsumerGroupMetadata,
  ConsumerOf,
  ConsumerRecord,
  IsolationLevel,
  WithSize
}
import com.evolutiongaming.skafka.producer.{Producer, ProducerConfig, ProducerOf}
import com.evolutiongaming.skafka.{CommonConfig, FromBytes, Offset, Partition, TopicPartition}
import munit.FunSuite
import scodec.bits.ByteVector

import scala.concurrent.duration.*

/** The transactional module owns the producer settings its design depends on: the stable per-partition
  * `transactional.id` (a takeover must abort a crashed owner's unfinished transaction) and idempotence - applied over
  * whatever `producerConfig` carries. Its recovery read is wired `read_committed` from earliest with the configured
  * deadline enabled, and its ephemeral consumers are group-less and never commit offsets - a committed offset would
  * override the earliest reset on the next recovery. Acquisition (of either module) also checks a mapper's declared
  * source partition count against the live input topic - see the source-partitions tests below.
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

  test("a partition mapper redirects the recovery read (owned keys only) but never the transactional.id") {
    // input partition 1 under a 2->1 modulo mapping: recovery reads the shared state partition 0 and keeps only
    // the keys input partition 1 owns, while the ids stay keyed to the input partition - a state-partition-keyed
    // transactional.id would be shared by live co-writers, whose inits would fence each other
    val mapper  = KafkaPersistencePartitionMapper.modulo(sourcePartitions = 2, statePartitions = 1)
    val inputP1 = Partition.unsafe(1)
    val inputTp = TopicPartition("input-topic", inputP1)
    val stateTp = TopicPartition("state-topic", Partition.min) // getStatePartition(1) under the 2->1 modulo

    // one key per owning input partition, both living in the shared state partition
    val candidates = (1 to 50).map(i => s"key$i")
    val ownedKey   = candidates.find(k => mapper.isStateKeyOwned(k, inputP1)).getOrElse(fail("no p1-owned key"))
    val unownedKey = candidates.find(k => mapper.isStateKeyOwned(k, Partition.min)).getOrElse(fail("no p0-owned key"))

    val fakes = new FakeConsumers(stateTp)

    def record(offset: Long, key: String): ConsumerRecord[String, ByteVector] =
      ConsumerRecord(
        topicPartition   = stateTp,
        offset           = Offset.unsafe(offset),
        timestampAndType = none,
        key              = WithSize(key).some,
        value            = WithSize(ByteVector.encodeUtf8(s"$key-value").toOption.get).some,
      )

    val test = for {
      capturedProducer  <- Ref.of[IO, Option[ProducerConfig]](none)
      capturedConsumers <- Ref.of[IO, List[ConsumerConfig]](Nil)
      positionRef       <- Ref.of[IO, Long](0L)
      readConsumer = fakes.consumer(
        endOffset   = 2L.pure[IO],
        positionRef = positionRef,
        records     = List(record(0, ownedKey), record(1, unownedKey)),
      )
      hwConsumer = fakes.consumer(
        endOffset   = 2L.pure[IO],
        positionRef = positionRef,
        records     = Nil,
      )
      inner = fakes.consumerOf(readConsumer = readConsumer, hwConsumer = hwConsumer)
      capturingOf = new ConsumerOf[IO] {
        def apply[K, V](
          config: ConsumerConfig
        )(implicit fromBytesK: FromBytes[IO, K], fromBytesV: FromBytes[IO, V]) =
          Resource.eval(capturedConsumers.update(_ :+ config)) *> inner(config)
      }
      producerOf = new ProducerOf[IO] {
        def apply(config: ProducerConfig): Resource[IO, Producer[IO]] =
          Resource.eval(capturedProducer.set(config.some)).as(Producer.empty[IO])
      }
      listed <- KafkaPersistenceModule
        .cachingTransactional[IO, String](
          consumerOf = capturingOf,
          producerOf = producerOf,
          config = KafkaPersistenceModule.TransactionalConfig(
            consumerConfig        = ConsumerConfig(common = CommonConfig(clientId = "client".some)),
            producerConfig        = ProducerConfig(common = CommonConfig(clientId = "client".some)),
            transactionalIdPrefix = "app",
            snapshotTopic         = "state-topic",
          ),
          assignment = PartitionAssignment[IO](
            topicPartition = inputTp,
            assignedAt     = Offset.min,
            groupMetadata  = IO.pure(none[ConsumerGroupMetadata]),
          ),
          partitionMapper = mapper,
        )
        .use(_.keysOf.all("app", "group", inputTp).toList)
      producerConfig  <- capturedProducer.get
      consumerConfigs <- capturedConsumers.get
    } yield {
      // only the keys owned by input partition 1 are recovered from the shared state partition
      assertEquals(listed.map(_.key), List(ownedKey))
      // the id stays input-keyed
      assertEquals(producerConfig.flatMap(_.transactionalId), "app-snapshot-1".some)
      assertEquals(producerConfig.flatMap(_.common.clientId), "client-snapshot-1".some)
      // the readers are input-keyed too, so co-owners of one state partition stay distinct in a single JVM. That
      // the read went to the mapped partition needs no assertion: the fake serves endOffsets for state partition 0
      // only, so an unmapped read would have failed with a missing offset. Exactly two consumers, and the absence of
      // a third is the point: this builds the module constructor directly, and the source-partitions check belongs to
      // the KafkaPersistenceModuleOf factories - its `-src` consumer appears there (see the tests below)
      assertEquals(consumerConfigs.flatMap(_.common.clientId), List("client-snapshot-1-hw", "client-snapshot-1"))
    }
    test.unsafeRunSync()
  }

  test("recovery warns exactly when a mapper owns none of a non-empty state partition") {
    // the level, not the wording: owning nothing of a partition that holds keys is the loudest sign of a mapper
    // disagreeing with the input topic's partitioning, and the docs promise a warn for it. The two negative cases
    // are what keep that promise from being noise - identity claims everything it reads, and an empty partition
    // is no evidence of anything
    def claiming(keys: String => Boolean): KafkaPersistencePartitionMapper = new KafkaPersistencePartitionMapper {
      def getStatePartition(sourcePartition: Partition): Partition               = Partition.min
      def isStateKeyOwned(stateKey: String, sourcePartition: Partition): Boolean = keys(stateKey)
    }

    // one recovery over a state partition holding `keys`: the warns it logged, and the keys it recovered. Both
    // consumer views are the same fake, so the bounds agree and the open-transaction wait - the read's other warn -
    // never fires, which is what makes counting warns equivalent to counting this one
    def recover(
      mapper: KafkaPersistencePartitionMapper,
      inputPartition: Partition,
      keys: List[String],
    ): IO[(Int, List[String])] = {
      val stateTp = TopicPartition("state-topic", Partition.min)
      val inputTp = TopicPartition("input-topic", inputPartition)
      val fakes   = new FakeConsumers(stateTp)
      val records = keys.zipWithIndex.map {
        case (key, index) =>
          ConsumerRecord[String, ByteVector](
            topicPartition   = stateTp,
            offset           = Offset.unsafe(index.toLong),
            timestampAndType = none,
            key              = WithSize(key).some,
            value            = WithSize(ByteVector.encodeUtf8(s"$key-value").toOption.get).some,
          )
      }
      for {
        logged      <- Ref.of[IO, List[String]](Nil)
        positionRef <- Ref.of[IO, Long](0L)
        consumer     = fakes.consumer(endOffset = keys.size.toLong, positionRef = positionRef, records = records)
        recovered <- {
          implicit val logOf: LogOf[IO] = LogOf.const(IO.pure(warnRecordingLog(logged)))
          KafkaPersistenceModule
            .cachingTransactional[IO, String](
              consumerOf = fakes.consumerOf(readConsumer = consumer, hwConsumer = consumer),
              producerOf = (_: ProducerConfig) => Resource.pure[IO, Producer[IO]](Producer.empty[IO]),
              config = KafkaPersistenceModule.TransactionalConfig(
                consumerConfig        = ConsumerConfig(),
                producerConfig        = ProducerConfig(),
                transactionalIdPrefix = "app",
                snapshotTopic         = stateTp.topic,
              ),
              assignment      = PartitionAssignment[IO](inputTp, Offset.min, IO.pure(none[ConsumerGroupMetadata])),
              partitionMapper = mapper,
            )
            .use(_.keysOf.all("app", "group", inputTp).toList)
        }
        count <- logged.get.map(_.size)
      } yield (count, recovered.map(_.key).sorted)
    }

    val two = List("k1", "k2")
    val test = for {
      ownsNone  <- recover(claiming(_ => false), Partition.unsafe(1), two)
      ownsOne   <- recover(claiming(_ == "k1"), Partition.unsafe(1), two)
      ownsAll   <- recover(KafkaPersistencePartitionMapper.identity, Partition.min, two)
      readsNone <- recover(claiming(_ => false), Partition.unsafe(1), Nil)
    } yield {
      // owning nothing of a partition that holds keys warns - and recovers nothing, which is what the warn says
      assertEquals(ownsNone, (1, List.empty[String]))
      // owning some is the healthy many-to-one shape: the co-owners' keys are always there, so it must stay quiet
      assertEquals(ownsOne, (0, List("k1")))
      assertEquals(ownsAll, (0, two), "identity owns every key it reads - nothing to warn about")
      assertEquals(readsNone, (0, List.empty[String]), "an empty state partition is no evidence of a wrong mapper")
    }
    TestControl.executeEmbed(test).unsafeRunSync()
  }

  // records warn lines only: these tests assert the level the recovery chose, never its wording
  private def warnRecordingLog(lines: Ref[IO, List[String]]): Log[IO] = new Log[IO] {
    def trace(msg: => String, mdc: Log.Mdc)                   = IO.unit
    def debug(msg: => String, mdc: Log.Mdc)                   = IO.unit
    def info(msg: => String, mdc: Log.Mdc)                    = IO.unit
    def warn(msg: => String, mdc: Log.Mdc)                    = lines.update(_ :+ msg)
    def warn(msg: => String, cause: Throwable, mdc: Log.Mdc)  = lines.update(_ :+ msg)
    def error(msg: => String, mdc: Log.Mdc)                   = IO.unit
    def error(msg: => String, cause: Throwable, mdc: Log.Mdc) = IO.unit
  }

}
