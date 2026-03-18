RELEASE_TYPE: patch

This release adds support for `HealthCheck`. A health check is a proactive error raised by Hegel when we detect your test is likely to have degraded testing power or performance. For example, `FilterTooMuch` is raised when too many test cases are filtered out by the rejection sampling of `.filter()` or `assume()`.

Health checks can be suppressed with the new `suppess_health_check` setting.
