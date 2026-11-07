# 0.5.0-SNAPSHOT

* Removed revelator-raft and Chronicle dependency management.
* Target Java 22, including the existing affinity runtime dependency. No preview flags are required.
* Added lifecycle states, bounded asynchronous drain, effective close, fail-stop worker supervision,
  publication-after-stop rejection and overflow-safe claim validation.
* Experimental HEADER_COMMIT uses successor invalidation, look-ahead gating and safe padding handoff.
* Removed incomplete scalable experiments from the runtime; source history is linked in docs/HISTORY.md.
* Replaced the cooperative loop with a bounded reusable-session pipeline, explicit frame positions,
  reset hook, shutdown callbacks and continuous actual completion publication.
* Handler failures are fatal across supported processors. Global reorder barriers also order empty signatures.
* Replaced unfinished payments/primes/crypto examples with basic and cooperative examples; history is linked.

Breaking changes: successful close now drains/joins; publish accepts complete claimed prefixes;
message type 31 is runtime-only; removed ExceptionHandler stub, unused compatibility shims and scalable
runtime classes. The thread factory adapter exposes physical-core pinning only. Re-run performance baselines: historical header/scalable/pipeline results
are not claims about the changed runtime. No new performance figures are claimed in this change.

Before a release tag: resolve the external affinity SNAPSHOT, verify on real JDK 22 and target Linux,
and run the release checklist. This development version is not a published 0.5 release.
