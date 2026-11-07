# Historical experiments

The pre-0.5 tree is commit a7adc42d67463028c1700a9efb97a56338400856. Original scalable work also lives in
origin/001-scalable-proc. Use git show or a separate checkout to inspect those experiments; unbuilt source
copies are deliberately not maintained in the main tree.

Payments was under revelator-examples/src/main/java/exchange/core2/revelator/examples/payments at that
commit. It is not a reference implementation: readiness/settlement and parallel-transfer semantics need
repair. The next payments work should be a tests-perf scenario against the current API, a common business
kernel and a sequential reference, not resurrection of an unbuilt archive. Primes/crypto are in that history too.
