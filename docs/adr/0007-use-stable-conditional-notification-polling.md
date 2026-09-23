# Use stable conditional notification polling

The daemon polls one stable global notifications URL with `all=true`, preserving its `Last-Modified` validator and obeying `X-Poll-Interval`. On changed responses it paginates newest-first through a durable overlap, then atomically commits discovered work, the high-water mark, and the new validator; newly registered repositories receive a one-time repository-scoped bootstrap because moving `since` parameters would invalidate the stable conditional representation.
