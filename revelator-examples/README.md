# Examples

BasicExample publishes 100 values and drains the runtime on close. CooperativeExample prepares private
session state and retries application until an external atomic permission is available. Both use ordinary
threads and need no native access flag. Native affinity is optional and described in the root README.

Legacy payments, primes and manual crypto workloads are referenced in ../docs/HISTORY.md and are not built or
advertised as reference implementations. In particular payments retains known readiness/settlement issues.
