RELEASE_TYPE: patch

When the hegel server process exits unexpectedly, the library now detects this immediately and fails with a clear error pointing to `.hegel/server.log`, instead of blocking for up to 120 seconds on the socket read timeout.
