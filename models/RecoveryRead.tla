------------------------- MODULE RecoveryRead -------------------------
(*******************************************************************************)
(* The snapshot-topic recovery read against Kafka's transactional log           *)
(* semantics: what bound makes a read_committed "read to the end" return every   *)
(* committed snapshot?  A focused semantics model, isolated from the refinement   *)
(* tower (like TokenSync), answering the question behind finding F-10.            *)
(*                                                                             *)
(* The log is a sequence of one-record transactions, each committed ("c"), open   *)
(* ("o") or aborted ("x").  Kafka semantics modeled:                              *)
(*   - LSO = the offset before the FIRST open record: what read_committed          *)
(*     endOffsets returns.  HW = the log end: the read_uncommitted end offset.     *)
(*   - A read_committed reader's position cannot pass an open record, so a read    *)
(*     bounded ABOVE one waits until the broker resolves it (Complete's guard);    *)
(*     TimeoutAbort is the broker's transaction.timeout.ms abort.                  *)
(*   - initTransactions on a transactional.id completes/aborts that id's previous  *)
(*     transaction before the new producer may write (KafkaProducer javadoc;       *)
(*     reader-visibility of the abort is external-semantics ext(K5)).              *)
(*                                                                             *)
(* Scenario (the double handover behind F-10): owner A commits snapshot S1,       *)
(* opens a transaction and hard-crashes leaving it open; owner B takes over,       *)
(* commits the newer snapshot S3, opens its own transaction and crashes too;       *)
(* reader C recovers.  Knobs:                                                     *)
(*   - StableId : A and B share the partition's transactional.id (stable per       *)
(*     partition), so B's mandatory init aborts A's open transaction before B      *)
(*     writes.  FALSE = unique per-assignment ids: nobody can abort A's            *)
(*     transaction; only the timeout.                                              *)
(*   - HwTarget : C bounds the read at the high watermark (captured through an     *)
(*     uncommitted-isolation lens) instead of its own read_committed endOffsets.   *)
(*   - Foreign  : a producer OUTSIDE the partition's id lineage leaves an open      *)
(*     transaction no takeover can abort -- the shared-snapshot-topic               *)
(*     misconfiguration the design documents as unsupported.                       *)
(*                                                                             *)
(* Results (the four configs):                                                    *)
(*   - recoveryread_lso_unique  VIOLATES INV_ReadsAllCommitted: C's own endOffsets  *)
(*     is the LSO, pinned by A's still-open transaction BELOW the committed S3;     *)
(*     the read completes early, silently missing S3 (the 85 ms live under-read).   *)
(*   - recoveryread_hw_unique   HOLDS: the HW bound makes C wait A's transaction     *)
(*     out (Kafka Streams' restore shape -- KAFKA-10167, ext(K6)).                  *)
(*   - recoveryread_lso_stable  HOLDS: B's init aborted A's transaction BEFORE B     *)
(*     wrote S3, so within one id lineage a committed record above an open           *)
(*     transaction is UNREACHABLE (INV_LineageSerialized) -- the plain LSO bound     *)
(*     is correct with no wait and no reader-side ordering assumption: C completes    *)
(*     at B's dangling transaction without waiting it out, missing nothing.           *)
(*   - recoveryread_lso_foreign VIOLATES INV_ReadsAllCommitted: the same under-read   *)
(*     reintroduced by a foreign open transaction -- what the one-topic-one-flow      *)
(*     discipline carries; no read bound decision can absorb a co-writer outside      *)
(*     the lineage (only the HW-wait turns it from silent to slow).                    *)
(*******************************************************************************)
EXTENDS Naturals, Sequences

CONSTANTS StableId, HwTarget, Foreign

VARIABLES log, phase, target, result
  \* log    : Seq([w : {"A","B","F"}, st : {"c","o","x"}])  -- one record per transaction
  \* phase  : the scenario script position (the double handover is a fixed cast)
  \* target : 0 = read bound not yet captured; else the captured bound (a log index)
  \* result : the set of log indexes the completed read returned

vars == <<log, phase, target, result>>

Rec(writer, status) == [w |-> writer, st |-> status]

FirstOpen(l) ==
  IF \E i \in DOMAIN l : l[i].st = "o"
    THEN CHOOSE i \in DOMAIN l : l[i].st = "o" /\ \A j \in DOMAIN l : l[j].st = "o" => i <= j
    ELSE Len(l) + 1

LsoOf(l) == FirstOpen(l) - 1   \* read_committed endOffsets: pinned by the first open txn
HwOf(l)  == Len(l)             \* read_uncommitted endOffsets: the log end

Init ==
  /\ log = <<>>
  /\ phase = "a1"
  /\ target = 0
  /\ result = {}

\* owner A: commits S1, then opens a transaction and hard-crashes (it stays open)
AWriteS1 ==
  /\ phase = "a1"
  /\ log' = Append(log, Rec("A", "c"))
  /\ phase' = "a2"
  /\ UNCHANGED <<target, result>>

AOpenT1 ==
  /\ phase = "a2"
  /\ log' = Append(log, Rec("A", "o"))
  /\ phase' = IF Foreign THEN "f" ELSE "binit"
  /\ UNCHANGED <<target, result>>

\* a producer outside the partition's id lineage (misconfiguration control)
ForeignOpen ==
  /\ phase = "f"
  /\ log' = Append(log, Rec("F", "o"))
  /\ phase' = "binit"
  /\ UNCHANGED <<target, result>>

\* owner B's MANDATORY initTransactions: with a stable id it aborts the lineage's open
\* transaction (A's) before B may write; with unique per-assignment ids it aborts nothing
BInit ==
  /\ phase = "binit"
  /\ log' = IF StableId
              THEN [i \in DOMAIN log |->
                     IF log[i].w = "A" /\ log[i].st = "o" THEN Rec("A", "x") ELSE log[i]]
              ELSE log
  /\ phase' = "bwrite"
  /\ UNCHANGED <<target, result>>

BWriteS3 ==
  /\ phase = "bwrite"
  /\ log' = Append(log, Rec("B", "c"))
  /\ phase' = "bopen"
  /\ UNCHANGED <<target, result>>

BOpenT2 ==
  /\ phase = "bopen"
  /\ log' = Append(log, Rec("B", "o"))
  /\ phase' = "read"
  /\ UNCHANGED <<target, result>>

\* reader C captures its bound once: its own read_committed end offset (the LSO), or the
\* high watermark through an uncommitted-isolation lens
Capture ==
  /\ phase = "read"
  /\ target = 0
  /\ target' = IF HwTarget THEN HwOf(log) ELSE LsoOf(log)
  /\ UNCHANGED <<log, phase, result>>

\* the broker's transaction.timeout.ms abort resolves an open transaction (any time before
\* the read completes; one per step)
TimeoutAbort ==
  /\ phase # "done"
  /\ \E i \in DOMAIN log :
       /\ log[i].st = "o"
       /\ log' = [log EXCEPT ![i] = Rec(log[i].w, "x")]
  /\ UNCHANGED <<phase, target, result>>

\* the read completes at its bound -- but a read_committed position cannot pass an open
\* record, so an open transaction BELOW the bound blocks completion (the wait)
Complete ==
  /\ phase = "read"
  /\ target > 0
  /\ \A i \in 1 .. target : log[i].st # "o"
  /\ result' = {i \in 1 .. target : log[i].st = "c"}
  /\ phase' = "done"
  /\ UNCHANGED <<log, target>>

Next ==
  \/ AWriteS1 \/ AOpenT1 \/ ForeignOpen \/ BInit \/ BWriteS3 \/ BOpenT2
  \/ Capture \/ TimeoutAbort \/ Complete

Spec == Init /\ [][Next]_vars /\ WF_vars(Next)

----------------------------------------------------------------------------
\* recovery is correct iff the completed read returned every committed snapshot
INV_ReadsAllCommitted ==
  (phase = "done") => \A i \in DOMAIN log : log[i].st = "c" => i \in result

\* the read never returns an uncommitted/aborted record ("c" is terminal, so this is stable)
INV_ReadCommittedOnly ==
  \A i \in result : i \in DOMAIN log /\ log[i].st = "c"

\* the structural fact the stable-id design rests on: within one id lineage (no foreign
\* writer), a committed record NEVER sits above an open transaction -- mandatory init
\* serializes the lineage, so the LSO bound needs no wait to be complete
INV_LineageSerialized ==
  (StableId /\ ~Foreign) =>
    \A i \in DOMAIN log : \A j \in DOMAIN log :
      (i < j /\ log[j].st = "c") => log[i].st # "o"

Terminates == <>(phase = "done")

TypeOK ==
  /\ log \in Seq([w : {"A", "B", "F"}, st : {"c", "o", "x"}])
  /\ Len(log) <= 5
  /\ phase \in {"a1", "a2", "f", "binit", "bwrite", "bopen", "read", "done"}
  /\ target \in 0 .. 5
  /\ result \subseteq 1 .. 5
=============================================================================
