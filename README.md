# lcmf-bus

Minimal frontend-oriented message bus for [`LCMF`](https://github.com/algebrain/lcmf-docs).

Current first version keeps only the parts that matter for the frontend:

- in-memory subscriptions
- envelope discipline
- required `:module` on publish
- `:parent-envelope` for derived events
- `correlation-id` and `causation-path`
- fail-fast validation of public API boundaries
- handler error isolation

It intentionally does not include server-side complexities like:

- buffering and backpressure
- transactional delivery
- persistence
- worker pools
- transport adapters

Core API:

- `make-bus`
- `publish!`
- `subscribe!`
- `unsubscribe!`
- `listener-count`

Testing workflow:

- `bb test.bb`
- `clojure -M:test`
- `clojure -M:test-watch`
