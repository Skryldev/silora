package storage

// HealthChecker implementations should be lightweight.
//
// Liveness and readiness can be layered above this later:
//   - liveness: process alive
//   - readiness: storage reachable and acceptable latency