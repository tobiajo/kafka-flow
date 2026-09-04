package com.evolutiongaming.kafka.flow.kafka

import scala.util.control.NoStackTrace

/** A transactional snapshot write or offset commit the broker rejected because the consumer generation it carried is
  * stale (KIP-447; kafka-clients raises it as `CommitFailedException`). The transaction aborted, so nothing landed and
  * no offset advanced.
  *
  * The rejection itself is the fence, and it needs no help from the flow: the producer stays usable, and the driving
  * consumer heals on its next poll - either completing the in-flight rebalance round under the new generation
  * (partitions it retained keep going) or rejoining, which tears the partition's flows down first. Failing the flow on
  * top would add a leave/rejoin of its own, which bumps the generation again and fences the peers - a rolling deploy
  * becomes a retry storm. So a fence within the writer's tolerance surfaces as this type: callers keep the key dirty or
  * the offset uncommitted and retry on their next tick. Past the tolerance the underlying error propagates and fails
  * the flow. See `docs/kafka-single-writer-design.md`.
  */
final case class GenerationFencedError(cause: Throwable)
    extends RuntimeException(s"fenced by a stale consumer generation: $cause", cause)
    with NoStackTrace
