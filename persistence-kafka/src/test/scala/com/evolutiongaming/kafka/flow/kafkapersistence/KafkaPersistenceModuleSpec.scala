package com.evolutiongaming.kafka.flow.kafkapersistence

import cats.effect.testkit.TestControl
import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Ref, Resource}
import cats.syntax.all.*
import com.evolutiongaming.catshelper.{Log, LogOf}
import com.evolutiongaming.kafka.flow.{FlowMetrics, PartitionAssignment}
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

  // acquires both module paths - the plain caching (via its factory, where the input topic is known) and the
  // transactional - with the same consumers and mapper: the source-partitions check must behave identically on both
  private def acquireBoth(
    consumerOf: ConsumerOf[IO],
    mapper: KafkaPersistencePartitionMapper,
    inputTp: TopicPartition,
  ): IO[List[Either[Throwable, Unit]]] = {
    val assignment = PartitionAssignment[IO](inputTp, Offset.min, IO.pure(none[ConsumerGroupMetadata]))
    // through the factories, both of them, because that is where the check lives - the lower-level
    // KafkaPersistenceModule constructors deliberately do not carry it
    val transactional = KafkaPersistenceModuleOf
      .cachingTransactional[IO, String](
        consumerOf = consumerOf,
        producerOf = (_: ProducerConfig) => Resource.pure[IO, Producer[IO]](Producer.empty[IO]),
        config = KafkaPersistenceModule.TransactionalConfig(
          consumerConfig        = ConsumerConfig(),
          producerConfig        = ProducerConfig(),
          transactionalIdPrefix = "app",
          snapshotTopic         = "state-topic",
        ),
        metrics         = FlowMetrics.empty[IO],
        partitionMapper = mapper,
      )
      .make(assignment)
    val plain = KafkaPersistenceModuleOf
      .caching[IO, String](
        consumerOf      = consumerOf,
        producer        = Producer.empty[IO],
        consumerConfig  = ConsumerConfig(),
        snapshotTopic   = "state-topic",
        metrics         = FlowMetrics.empty[IO],
        partitionMapper = mapper,
      )
      .make(assignment)
    List(transactional, plain).traverse(_.use_.attempt)
  }

  test("acquisition fails when the live input topic disagrees with the mapper's declared partition count") {
    // the silent alternative is corruption at recovery: keys hashed against the wrong count are credited to the
    // wrong input partition, whose owner rebuilds them from empty while a co-owner may recover duplicates
    val mapper = KafkaPersistencePartitionMapper.modulo(sourcePartitions = 4, statePartitions = 1)
    val fakes  = new FakeConsumers(TopicPartition("state-topic", Partition.min))
    val test = for {
      positionRef <- Ref.of[IO, Long](0L)
      consumer =
        fakes.consumer(0L.pure[IO], positionRef, Nil, partitionsByTopic = Map("input-topic" -> IO.pure(2)))
      results <- acquireBoth(
        fakes.consumerOf(readConsumer = consumer, hwConsumer = consumer),
        mapper,
        TopicPartition("input-topic", Partition.min),
      )
    } yield results.foreach {
      case Left(e: KafkaPersistenceModule.SourcePartitionsMismatchError) =>
        // an operator gets both numbers, the topic and the mapper - enough to see which side to fix
        assertEquals((e.inputTopic, e.expected, e.actual), ("input-topic", 4, 2))
        List("input-topic", "4", "2", mapper.toString).foreach(part => assert(clue(e.getMessage).contains(clue(part))))
      case other => fail(s"expected acquisition to fail with the mismatch, got $other")
    }
    test.unsafeRunSync()
  }

  test("acquisition passes the check when the live input topic matches the declared partition count") {
    // also pins WHICH topic is queried: the fake serves a count for the input topic alone, so a check against
    // any other topic would read the empty delegate and fail as a mismatch
    val mapper = KafkaPersistencePartitionMapper.modulo(sourcePartitions = 2, statePartitions = 1)
    val fakes  = new FakeConsumers(TopicPartition("state-topic", Partition.min))
    val test = for {
      positionRef <- Ref.of[IO, Long](0L)
      consumer =
        fakes.consumer(0L.pure[IO], positionRef, Nil, partitionsByTopic = Map("input-topic" -> IO.pure(2)))
      results <- acquireBoth(
        fakes.consumerOf(readConsumer = consumer, hwConsumer = consumer),
        mapper,
        TopicPartition("input-topic", Partition.min),
      )
    } yield assertEquals(results, List(Right(()), Right(())))
    test.unsafeRunSync()
  }

  test("a mapper declaring no expected count skips the check - acquisition opens no consumer") {
    // a custom implementation inherits the None default and stays unvalidated, as before this member existed;
    // unusedConsumerOf raises on any consumer opening, so passing is proof no metadata was fetched
    val unchecked = new KafkaPersistencePartitionMapper {
      def getStatePartition(sourcePartition: Partition): Partition               = Partition.min
      def isStateKeyOwned(stateKey: String, sourcePartition: Partition): Boolean = true
    }
    val test = acquireBoth(unusedConsumerOf, unchecked, TopicPartition("input-topic", Partition.min))
      .map(results => assertEquals(results, List(Right(()), Right(()))))
    test.unsafeRunSync()
  }

  test("a failed partition-count fetch propagates as itself, never as a mismatch") {
    // unknown is not wrong: a broker that cannot be asked must fail the assignment with its own error, not
    // convict the mapper of a mismatch it never verified
    val boom   = new RuntimeException("metadata fetch failed")
    val mapper = KafkaPersistencePartitionMapper.modulo(sourcePartitions = 2, statePartitions = 1)
    val fakes  = new FakeConsumers(TopicPartition("state-topic", Partition.min))
    val test = for {
      positionRef <- Ref.of[IO, Long](0L)
      consumer =
        fakes.consumer(0L.pure[IO], positionRef, Nil, partitionsByTopic = Map("input-topic" -> IO.raiseError(boom)))
      results <- acquireBoth(
        fakes.consumerOf(readConsumer = consumer, hwConsumer = consumer),
        mapper,
        TopicPartition("input-topic", Partition.min),
      )
    } yield results.foreach {
      case Left(e)  => assert(clue(e) eq clue[Throwable](boom))
      case Right(_) => fail("expected the fetch failure to fail acquisition")
    }
    test.unsafeRunSync()
  }

  test("a topic the broker reports no partitions for skips the check rather than failing the assignment") {
    // an empty answer is how a client reports a topic it does not know, and it cannot mean the configuration is
    // wrong: this consumer was assigned a partition of that topic, so the topic exists and the view is merely stale.
    // Failing here would be paid for by the whole consumer - the error leaves the rebalance callback and the flow's
    // retry rebuilds everything - so a transient view must not cost that
    val mapper = KafkaPersistencePartitionMapper.modulo(sourcePartitions = 2, statePartitions = 1)
    val fakes  = new FakeConsumers(TopicPartition("state-topic", Partition.min))
    val test = for {
      positionRef <- Ref.of[IO, Long](0L)
      consumer     = fakes.consumer(0L.pure[IO], positionRef, Nil, partitionsByTopic = Map("input-topic" -> 0.pure[IO]))
      results <- acquireBoth(
        fakes.consumerOf(readConsumer = consumer, hwConsumer = consumer),
        mapper,
        TopicPartition("input-topic", Partition.min),
      )
    } yield results.foreach {
      case Right(()) => ()
      case Left(e)   => fail(s"expected an unknown topic to skip the check, not fail the assignment: $e")
    }
    test.unsafeRunSync()
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
