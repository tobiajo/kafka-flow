package com.evolutiongaming.kafka.flow.kafkapersistence

import cats.data.{NonEmptyList, NonEmptyMap}
import cats.effect.unsafe.IORuntime
import cats.effect.{IO, Ref, Resource}
import cats.syntax.all.*
import com.evolutiongaming.catshelper.{FromTry, Log, LogOf}
import com.evolutiongaming.kafka.flow.{ForAllKafkaSuite, KafkaKey}
import com.evolutiongaming.skafka.consumer.{
  AutoOffsetReset,
  ConsumerConfig,
  ConsumerGroupMetadata,
  ConsumerOf,
  IsolationLevel
}
import com.evolutiongaming.skafka.producer.{Producer, ProducerConfig, ProducerOf, ProducerRecord}
import com.evolutiongaming.skafka.{
  ClientMetric,
  CommonConfig,
  Offset,
  OffsetAndMetadata,
  Partition,
  PartitionInfo,
  Topic,
  TopicPartition
}
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig}
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.errors.TimeoutException
import org.testcontainers.DockerClientFactory

import java.util.Properties
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.*

/** Verifies what actually happens when `commitTransaction` fails AMBIGUOUSLY (a client-side timeout: the broker may
  * have committed) and the code reacts by calling `abortTransaction` - the reaction
  * `KafkaSnapshotWriteDatabase.GroupCommit.commitBatch` originally had for every failure.
  *
  * kafka-clients (4.3.0, read from source) keeps an unacked `pendingTransition` for the timed-out `commitTransaction`
  * (`TransactionManager.handleCachedTransactionRequestResult`); until that commit is retried to completion every OTHER
  * transactional operation on the producer throws `IllegalStateException("Cannot attempt operation `X` because the
  * previous call to `commitTransaction` timed out and must be retried")`. So the blind abort:
  *
  *   - does NOT abort anything - it throws the above `IllegalStateException` (which `commitBatch` swallowed with
  *     `voidError`), while the in-flight EndTxn(COMMIT) still completes broker-side: the caller is told the write
  *     failed although it durably lands, offsets included;
  *   - leaves the producer unusable for NEW transactions - every later `beginTransaction` throws the same
  *     `IllegalStateException` until either `commitTransaction` is retried on this instance or the producer is rebuilt.
  *
  * The Java client's contract (KafkaProducer.commitTransaction javadoc): "It is safe to retry in either case, but it is
  * not possible to attempt a different operation (such as abortTransaction) since the commit may already be in the
  * progress of completing. If not retrying, the only option is to close the producer."
  *
  * The first test pins the client behaviour with the exact operation sequence `commitBatch` issues; the second drives
  * the real `KafkaSnapshotWriteDatabase.transactional` machinery and pins the same consequences through it: the
  * ambiguous timeout surfaces to the caller even though the transaction lands, and the producer is left spent (every
  * later write on it fails) until the module - and with it the producer - is rebuilt. `GroupCommit`'s existing blind
  * abort neither causes nor prevents any of this, which is why no production change follows from it.
  *
  * The broker outage is simulated by pausing the testcontainers broker (each suite has its own container), which is
  * fully under test control - no racy fault injection.
  */
class AmbiguousCommitAbortSpec extends ForAllKafkaSuite {
  implicit val ioRuntime: IORuntime = IORuntime.global
  implicit val logOf: LogOf[IO]     = LogOf.slf4j[IO].unsafeRunSync()
  implicit val log: Log[IO]         = logOf(this.getClass).unsafeRunSync()
  implicit val fromTry: FromTry[IO] = FromTry.lift

  // broker pause/unpause cycles plus transactional awaits run past munit's 30s default
  override def munitTimeout: Duration = 3.minutes

  private val appId = "app-id"

  private def commonConfig(clientId: String) = CommonConfig(
    bootstrapServers = NonEmptyList.one(kafka.container.bootstrapServers),
    clientId         = clientId.some,
  )

  /** Short `max.block.ms` so an unanswerable commit turns into the ambiguous client-side TimeoutException quickly. */
  private def transactionalProducer(transactionalId: String): Resource[IO, Producer[IO]] =
    ProducerOf
      .apply1[IO]()
      .apply(
        ProducerConfig(
          common          = commonConfig(s"$transactionalId-producer"),
          transactionalId = transactionalId.some,
          idempotence     = true,
          maxBlock        = 3.seconds,
        )
      )

  private def pauseBroker: IO[Unit] =
    IO.blocking(DockerClientFactory.instance().client().pauseContainerCmd(kafka.container.containerId).exec()).void

  private def unpauseBroker: IO[Unit] =
    IO.blocking(DockerClientFactory.instance().client().unpauseContainerCmd(kafka.container.containerId).exec())
      .void
      .voidError // safety net: idempotent cleanup must not mask a test failure

  private def readCommittedSnapshots(stateTopic: String): IO[BytesByKey] =
    KafkaPartitionPersistence.readSnapshots[IO](
      consumerOf = ConsumerOf.apply1[IO](),
      consumerConfig = ConsumerConfig(
        common          = commonConfig("read-committed-observer"),
        autoCommit      = false,
        autoOffsetReset = AutoOffsetReset.Earliest,
        isolationLevel  = IsolationLevel.ReadCommitted,
      ),
      snapshotTopic = stateTopic,
      partition     = Partition.min,
      stall         = none,
    )

  private def committedOffset(group: String, inputTopic: String): IO[Option[Long]] =
    IO.blocking {
      val props = new Properties
      props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.container.bootstrapServers)
      val client = AdminClient.create(props)
      try {
        val offsets = client.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(10, TimeUnit.SECONDS)
        Option(offsets.get(new org.apache.kafka.common.TopicPartition(inputTopic, 0))).map(_.offset())
      } finally client.close()
    }

  private def eventually[A](hint: String, timeout: FiniteDuration = 60.seconds)(fa: IO[Option[A]]): IO[A] = {
    def loop(remaining: FiniteDuration): IO[A] = fa.flatMap {
      case Some(a)                     => a.pure[IO]
      case None if remaining > 0.nanos => IO.sleep(250.millis) *> loop(remaining - 250.millis)
      case None => IO.raiseError(new RuntimeException(s"condition not met within $timeout: $hint"))
    }
    loop(timeout)
  }

  private def utf8(bytes: scodec.bits.ByteVector): Option[String] = bytes.decodeUtf8.toOption

  test("kafka-clients contract: a blind abort after an ambiguous commit throws, while the commit still lands") {
    val stateTopic = "ambiguous-commit-contract-state-topic"
    val inputTopic = s"input-$stateTopic"
    val group      = "ambiguous-commit-contract-group"
    val inputTp    = TopicPartition(inputTopic, Partition.min)

    val test = for {
      _ <- createTopic(inputTopic, 1)
      _ <- createTopic(stateTopic, 1)
      _ <- withJoinedConsumer(group, inputTopic) { meta =>
        transactionalProducer(s"$group-tx").use { producer =>
          val transaction = for {
            _ <- producer.initTransactions
            // the exact sequence GroupCommit.commitBatch issues: begin, send, offsets, commit
            _ <- producer.beginTransaction
            _ <- producer
              .send(ProducerRecord[String, String](stateTopic, "ambiguous-value".some, "key1".some))
              .flatten
              .void
            _ <- producer.sendOffsetsToTransaction(
              NonEmptyMap.of(inputTp -> OffsetAndMetadata(Offset.unsafe(42))),
              meta
            )

            // freeze the broker: the EndTxn(COMMIT) can be sent but never answered within max.block.ms
            _       <- pauseBroker
            commit1 <- producer.commitTransaction.attempt

            // the commit failed AMBIGUOUSLY: a client-side timeout, the request may still complete broker-side
            _ = assert(
              commit1.left.exists(_.isInstanceOf[TimeoutException]),
              s"expected the paused-broker commit to fail with a client-side TimeoutException, got $commit1",
            )

            // the blind abort of the original commitBatch: it does not abort anything - the client refuses to
            // switch operations while the timed-out commit is pending. commitBatch's `voidError` swallowed
            // exactly this exception.
            abort1 <- producer.abortTransaction.attempt
            _ = assert(
              abort1.left.exists { e =>
                e.isInstanceOf[IllegalStateException] &&
                e.getMessage.contains("abortTransaction") &&
                e.getMessage.contains("commitTransaction` timed out and must be retried")
              },
              s"expected abort-after-ambiguous-commit to throw the client's IllegalStateException, got $abort1",
            )

            _ <- unpauseBroker

            // consequence 1: the caller was told the write failed, but the transaction completes broker-side -
            // snapshot AND bound input offset land, visible to a read_committed recovery
            _ <- eventually("the 'failed' snapshot write became visible to read_committed") {
              readCommittedSnapshots(stateTopic).map(_.get("key1").flatMap(utf8).filter(_ == "ambiguous-value"))
            }
            _ <- eventually("the 'failed' offset commit landed") {
              committedOffset(group, inputTopic).map(_.filter(_ == 42L))
            }

            // consequence 2: the producer is unusable for new transactions even though the broker committed -
            // in the original commitBatch every subsequent write on the partition dies here until the module
            // (and with it the producer) is rebuilt
            begin2 <- producer.beginTransaction.attempt
            _ = assert(
              begin2.left.exists { e =>
                e.isInstanceOf[IllegalStateException] &&
                e.getMessage.contains("beginTransaction") &&
                e.getMessage.contains("commitTransaction` timed out and must be retried")
              },
              s"expected beginTransaction after the unresolved commit to throw, got $begin2",
            )

            // the client's contract in action: retrying the commit resolves the ambiguity (the pending EndTxn
            // is re-awaited) and the producer becomes usable again - the basis of the fix
            _ <- producer.commitTransaction
            _ <- producer.beginTransaction
            _ <- producer.abortTransaction
          } yield ()

          // unpause on any failure path so the shared-suite broker never stays frozen
          transaction.guarantee(unpauseBroker)
        }
      }
    } yield ()

    test.unsafeRunSync()
  }

  /** Wraps a producer so the broker is paused just before the FIRST `commitTransaction`, turning exactly that commit
    * ambiguous; the broker is unpaused in the background a moment later, emulating a transient outage.
    */
  private def pausingBeforeFirstCommit(underlying: Producer[IO], unpauseAfter: FiniteDuration): IO[Producer[IO]] =
    Ref.of[IO, Boolean](true).map { first =>
      new Producer[IO] {
        def initTransactions: IO[Unit] = underlying.initTransactions
        def beginTransaction: IO[Unit] = underlying.beginTransaction
        def commitTransaction: IO[Unit] =
          first.getAndSet(false).flatMap { isFirst =>
            IO.whenA(isFirst) {
              pauseBroker *> (IO.sleep(unpauseAfter) *> unpauseBroker).start.void
            }
          } *> underlying.commitTransaction
        def abortTransaction: IO[Unit] = underlying.abortTransaction
        def sendOffsetsToTransaction(
          offsets: NonEmptyMap[TopicPartition, OffsetAndMetadata],
          consumerGroupMetadata: ConsumerGroupMetadata
        ): IO[Unit] = underlying.sendOffsetsToTransaction(offsets, consumerGroupMetadata)
        def send[K, V](record: ProducerRecord[K, V])(
          implicit toBytesK: com.evolutiongaming.skafka.ToBytes[IO, K],
          toBytesV: com.evolutiongaming.skafka.ToBytes[IO, V]
        ): IO[IO[com.evolutiongaming.skafka.producer.RecordMetadata]] = underlying.send(record)
        def partitions(topic: Topic): IO[List[PartitionInfo]]   = underlying.partitions(topic)
        def flush: IO[Unit]                                     = underlying.flush
        def clientMetrics: IO[Seq[ClientMetric[IO]]]            = underlying.clientMetrics
        def clientInstanceId(timeout: FiniteDuration): IO[Uuid] = underlying.clientInstanceId(timeout)
      }
    }

  test("an ambiguous commit surfaces without an abort: the transaction still lands, and the producer is spent") {
    val stateTopic = "ambiguous-commit-groupcommit-state-topic"
    val inputTopic = s"input-$stateTopic"
    val group      = "ambiguous-commit-groupcommit-group"
    val inputTp    = TopicPartition(inputTopic, Partition.min)
    val key        = KafkaKey(appId, group, inputTp, "key1")

    val test = for {
      _ <- createTopic(inputTopic, 1)
      _ <- createTopic(stateTopic, 1)
      _ <- withJoinedConsumer(group, inputTopic) { meta =>
        transactionalProducer(s"$group-tx").use { producer =>
          val scenario = for {
            _       <- producer.initTransactions
            pausing <- pausingBeforeFirstCommit(producer, unpauseAfter = 5.seconds)
            tx <- KafkaSnapshotWriteDatabase.transactional[IO, String](
              snapshotTopicPartition  = TopicPartition(stateTopic, Partition.min),
              producer                = pausing,
              inputTopicPartition     = inputTp,
              groupMetadata           = IO.pure(meta.some),
              assignedOffset          = Offset.min,
              maxWritesPerTransaction = KafkaPersistenceModule.TransactionalConfig.DefaultMaxWritesPerTransaction,
            )
            // the first write's commit times out client-side (broker paused): ambiguous, and reported to the
            // caller verbatim. No abort is attempted - the client would throw rather than abort while a commit
            // is unresolved, which is the bug this replaced
            first <- tx.writeDatabase.persist(key, "value-1").attempt
            // yet the transaction lands once the broker returns, so the "failure" cost no data: the input offset
            // rode the same transaction, so snapshot and offset are still together
            _ <- eventually("the ambiguously-committed snapshot is visible to read_committed") {
              readCommittedSnapshots(stateTopic).map(_.get(key.key).flatMap(utf8).filter(_ == "value-1"))
            }
            // and this producer is spent: the client refuses every further transaction while that commit stays
            // unresolved. That is why the surfaced failure has to tear the module down instead of carrying on -
            // only a new producer's initTransactions clears it
            second <- tx.writeDatabase.persist(key, "value-2").attempt
          } yield {
            assert(first.left.exists(_.isInstanceOf[TimeoutException]), s"the ambiguous timeout surfaced: $first")
            assert(second.isLeft, s"the producer is spent after an unresolved commit: $second")
          }

          scenario.guarantee(unpauseBroker)
        }
      }
    } yield ()

    test.unsafeRunSync()
  }
}
