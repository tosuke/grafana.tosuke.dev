# JWK Key Management Time Model

## Purpose

The Grafana Durable Object owns the JWT signing keyring. Workers obtain the current private key
through RPC and sign Grafana authentication tokens locally, while Grafana obtains the public keys
from the Worker's JWKS endpoint.

Keys rotate lazily. There is no alarm or scheduled job: every signing-key or JWKS request first
checks the keyring and performs any required initialization, cleanup, or rotation in a Durable
Object storage transaction.

## Time parameters

| Parameter              |            Value | Meaning                                                                        |
| ---------------------- | ---------------: | ------------------------------------------------------------------------------ |
| Signing lifetime       |          3 hours | How long an activated key may be used to sign new JWTs.                        |
| Grafana JWKS cache TTL |           1 hour | How long Grafana may retain a fetched JWKS without refreshing it.              |
| JWT lifetime           |       15 minutes | The validity period of each JWT issued to Grafana.                             |
| Clock skew allowance   |        5 minutes | Margin applied to publication and verification boundaries.                     |
| Ready lead time        | 1 hour 5 minutes | `JWKS cache TTL + clock skew`; the minimum publication time before activation. |
| Verification grace     |       20 minutes | `JWT lifetime + clock skew`; the time a retired key remains verifiable.        |

All timestamps stored in `jwk_keys` are Unix time in milliseconds.

## Keyring roles

The singleton row in `jwk_keyring` refers to at most three keys:

- `previous`: no longer signs tokens, but remains published while tokens signed by it may still be
  valid.
- `current`: the only key returned for signing.
- `next`: already published in JWKS, but not eligible for signing until `ready_at`.

The JWKS response contains every referenced key whose `verify_until` is either unset or still in
the future. It therefore normally contains two keys after initialization and three keys during the
verification grace period following a rotation.

Private key material is returned only for `current`. The JWKS endpoint exposes public JWKs only.

## Per-key timestamps

- `created_at`: when the key pair was generated.
- `ready_at`: the earliest time at which a pre-published key may become `current`.
- `activated_at`: when the key became `current`; unset for `next`.
- `sign_until`: the exclusive end of the key's signing window; unset for `next`.
- `verify_until`: the exclusive end of the key's JWKS publication window; unset for `next`.

`ready_at` is not a token claim and is not returned to Grafana. It is local keyring state that
protects against activating a key before Grafana's cached JWKS has had enough time to refresh.

## Initialization

On the first request at time `T0`, the store creates two keys:

```text
current: activated_at = T0
         sign_until  = T0 + 3h
         verify_until = T0 + 3h + 20m

next:    ready_at = T0 + 1h + 5m
         activated_at, sign_until, verify_until = null
```

Both public keys are returned immediately in JWKS. The `next` key therefore has at least one full
Grafana cache lifetime, plus clock-skew allowance, to reach Grafana before it can be activated.

## Normal rotation

Rotation occurs on the first request for which both conditions are true:

```text
now >= current.sign_until
now >= next.ready_at
```

The transaction then performs the following state change:

```text
previous <- current
current  <- next
next     <- newly generated key
```

At the rotation time `T1`:

- the promoted key receives `activated_at = T1`;
- its signing window becomes `[T1, T1 + 3h)`;
- its publication window ends at `T1 + 3h + 20m`;
- the newly generated `next` key receives `ready_at = T1 + 1h + 5m`; and
- the keyring generation is incremented.

The old `current` retains its existing `verify_until`. It remains in JWKS as `previous` until that
time, covering every JWT it could have signed plus the clock-skew allowance.

## Cleanup

Before evaluating rotation, the store removes `previous` when:

```text
now >= previous.verify_until
```

The key is removed from both the keyring and `jwk_keys`. No token that is still within the modeled
JWT lifetime and clock-skew allowance should require it after this boundary.

## Timeline example

For initialization at 00:00 and requests arriving exactly at the relevant boundaries:

```text
00:00  A becomes current; B is generated and published as next
01:05  B is eligible for promotion, but A continues signing
03:00  A's signing window ends; B becomes current; C is generated and published
03:20  A's verification window ends and A may be removed
04:05  C is eligible for promotion, but B continues signing
06:00  B's signing window ends; C becomes current
```

The 3-hour signing lifetime is intentionally longer than the 1-hour-5-minute ready lead time, so
under normal operation `next` is ready well before `current` expires.

## Delayed requests and boundary conditions

Lazy rotation means timestamps are based on the request that actually performs a transition, not
on a periodic schedule. If no request arrives at 03:00 in the example above and the next request
arrives at 04:30, rotation happens at 04:30 and the promoted key receives a fresh 3-hour signing
window beginning then.

If `current` has not expired, no rotation occurs even when `next` is already ready. Conversely, a
key must never be promoted while `now < next.ready_at`, because Grafana may still have a cached JWKS
that does not contain it.

The state `current.sign_until <= now < next.ready_at` should not arise from the normal timing model:
the ready lead time is 1 hour 5 minutes, while the signing lifetime is 3 hours. The current
implementation preserves `current` and does not rotate if this invariant is violated. This is a
defensive safety choice that avoids signing with an unpublished key; it should be treated as an
abnormal state because the returned signing lease has already expired.

## JWT claims

Each JWT has a 15-minute expiration and identifies its signing key with `kid`. No `nbf` claim is
required by this model: the token is valid immediately when issued, and key activation timing is
enforced by the key store rather than encoded into individual tokens.

The Worker caches the imported current private key only until `sign_until`. After that boundary it
asks the Durable Object for a new signing lease, which also triggers lazy rotation when eligible.

## Persistence and concurrency

The key material and keyring pointers are stored in the Grafana Durable Object's SQLite storage in
the `jwk_keys` and `jwk_keyring` tables. Initialization, cleanup, and rotation run in an asynchronous
Durable Object storage transaction, so concurrent requests observe a single committed keyring
transition. Rotation logs are emitted after the transaction commits and contain key identifiers and
timestamps, never private key material.

