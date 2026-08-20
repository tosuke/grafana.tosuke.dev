# JWK Key Management

## Purpose

The Grafana Durable Object owns the Ed25519 key used to sign the JWT that carries the Cloudflare
Access identity to Grafana. Workers obtain the private key through RPC and sign tokens locally;
Grafana obtains the corresponding public key from the Worker's JWKS endpoint.

This key is intentionally persistent. It is not rotated on a timer or by an alarm. The key is
generated lazily on the first signing-key or JWKS request, stored in the Durable Object's SQLite
database, and returned for all subsequent requests.

## Time parameters

| Parameter              |      Value | Meaning                                                            |
| ---------------------- | ---------: | ------------------------------------------------------------------ |
| Grafana JWKS cache TTL | 15 minutes | How long Grafana may retain the fetched JWKS before refreshing it. |
| JWT lifetime           | 15 minutes | The validity period of each JWT issued to Grafana.                 |

The JWKS response also advertises a 15-minute `Cache-Control` max age. The key itself has no
expiration time, so there is no signing-window or verification-grace timestamp to maintain.

## Storage model

The version-2 schema has one row in `jwk_keys`:

- `kid`: the random key identifier included in JWT headers and the public JWK;
- `private_key_pkcs8`: the Ed25519 private key used by Workers for local signing;
- `public_jwk`: the public key returned by the JWKS endpoint; and
- `created_at`: the key-generation timestamp.

The `singleton = 1` constraint ensures that the table contains at most one key. The private key
is returned only by `getSigningKey`; `getJWKSet` returns a JWKS containing only the public JWK.
JWKS construction uses `jose` so the response has the standard JWKS shape.

Migration 2 removes the previous `jwk_keyring` and rotating `jwk_keys` schema before creating this
single-key table. The endpoint is versioned as `/jwks/v2.json`, so Grafana does not reuse the
previous endpoint's cached JWKS after deployment.

## Initialization and requests

On the first request, the store checks `jwk_keys` inside an asynchronous Durable Object storage
transaction. If the row is absent, it generates and inserts one key. Concurrent first requests
are serialized by the transaction and observe the same key. Once the row exists, neither signing
requests nor JWKS requests generate a replacement key.

The Worker keeps the imported private `CryptoKey` in a single in-memory promise for up to 15
minutes. It then reloads the key from the Durable Object. This short cache bounds how long an
existing Worker isolate can continue using a key after a manual reset; a new isolate loads the
same PKCS#8 key from the Durable Object immediately.

JWTs continue to use a 15-minute expiration and include the key's `kid`. No `nbf` claim is needed:
the key is valid immediately after initialization and remains valid until a manual reset.

## Manual reset

Deleting the row from `jwk_keys` is the key-reset operation. The next signing-key or JWKS request
generates and stores a new key. Once Grafana refreshes JWKS, tokens signed with the old key can no
longer be validated.

Grafana may continue using its cached old JWKS for up to 15 minutes. Therefore a reset can cause
authentication failures during that interval: Workers may sign with the new key while Grafana has
not fetched it yet. Wait at least the configured 15-minute cache TTL after deleting the row before
expecting all requests to work again. The `/jwks/v2.json` endpoint itself is also cacheable for
15 minutes.

The reset should be performed deliberately from the Cloudflare dashboard or another authorized
Durable Object SQL operation. It is not part of normal request handling.

## Security and operations

The private key is confined to the Grafana Durable Object's SQLite storage and the Worker isolate
that signs the authentication request. The JWKS endpoint exposes only the public key. Since this
key authenticates requests to an internal Grafana path, the impact of key disclosure is limited by
that network boundary, but disclosure would still allow JWT forgery for that path until the key is
reset.

Initialization is logged with the `kid` and creation time only; private key material is never
logged. The key store is exposed as one `RpcTarget` adapter through `jwkStore()`, and RPC results
that contain key material are disposed by the caller after the final pipelined RPC result.
