# Release checklist

1. Verify supported API, ordering, borrowing and lifecycle contracts in API.md against current behavior.
2. Run mvn verify with JDK 22 and the deployed JDK on Linux and Windows; execute the native affinity
   adapter on supported target hardware with native access enabled. Do not infer OS support from compilation.
3. Publish/pin a released affinity dependency, then build with an empty Maven cache and no adjacent checkouts.
4. Install the artifacts and build/run an external consumer using only declared dependencies and BasicExample.
5. Record commit, JVM/flags, NUMA/CPU placement, workloads, capacity units and completion semantics for
   performance comparisons. Supported cursor and experimental variants must not be merged into one claim.
6. Set final coordinates/release notes only after checks pass. No release publication/tag is part of this change.
