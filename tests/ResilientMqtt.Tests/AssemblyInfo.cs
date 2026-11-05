// Integration tests use real sockets + timing; run them one at a time so they never compete for the CPU.
[assembly: CollectionBehavior(DisableTestParallelization = true)]
