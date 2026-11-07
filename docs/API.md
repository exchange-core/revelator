# Runtime contract

* Single producer: claim, payload writes and publish are one producer's responsibility. Shutdown callers
  must first stop/join that producer and publish outstanding claims; shutdown is not a second concurrent producer.
* `capacityLongs` and `payloadLongs` count 64-bit words. claim returns payload start; publish takes an endExclusive on a complete frame boundary, including a prefix of several claims. Correlation IDs have 56 bits. Application message types are 1..30;
  31 is reserved for runtime shutdown. Type 30 has no special runtime meaning.
* All earlier claims must be fully filled before the next claim: admission may publish them for progress.
  Ordinary stores and writeLongDataUnsafe are trusted low-level writes, not checked message transactions.
  If filling fails, abort the runtime rather than publishing partial data. There is no transactional rollback.
* Simple/Reordering handler `globalOffset` is frame start. Pipelined sessions expose frameStart and
  endExclusive explicitly; no legacy offset alias remains.
* Handler/session/buffer references are borrowed. Do not use them after completion. Override session.reset()
  to clear custom fields on reuse; leave scheduler metadata unchanged. false means retry with no repeated
  observable effect. onShutdown executes once only on a successful graceful drain, not on abort/failure.
* Reordering signature includes every conflict key; collisions can conservatively reduce concurrency.
  Signature -1 is global, including against zero-signature operations. Different-key effects must commute.
* NEW -> RUNNING -> STOPPING -> TERMINATED, with FAILED possible on startup, processing, abort or timeout.
  start is one-shot. close before start terminates without launching workers. stopAsync is idempotent;
  callers receive copies of the termination future and cannot complete the runtime's internal future.
* Worker failure does not advance its fence. Peers are interrupted; failed/partial business effects require
  application recovery. A timeout cannot forcibly stop a handler that ignores interruption.
* Full-ring admission spins and checks failure/interruption. Use a bounded external admission policy if
  application-level tryClaim is required; this version does not provide nonblocking claims.
* Standard CURSOR supports simple/cooperative/reordering processors. HEADER_COMMIT is experimental and requires successor invalidation, initial marker initialization,
  padding handoff and ownership of the look-ahead header. It is opt-in through PublicationMode. Scalable
  sources are outside the runtime artifact. LINE_ALIGNED and reordering retain experimental status.

A processor factory must return a nonempty graph and a gating fence covering every reader of borrowed data.
Custom processors must cooperate with interruption and publish only their actual completed prefix.
