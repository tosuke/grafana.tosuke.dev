import { describe, expect, it } from "vitest";
import { env } from "cloudflare:workers";
import { runInDurableObject } from "cloudflare:test";
import { CLOCK_SKEW, DOJWKStore, JWT_LIFETIME, KEY_SIGNING_LIFETIME, JWKS_CACHE_TTL } from "./jwk";

const KEY_VERIFY_GRACE = JWT_LIFETIME + CLOCK_SKEW;

type KeyringRow = {
  previous_kid: string | null;
  current_kid: string;
  next_kid: string;
  generation: number;
};

function keyring(state: DurableObjectState): KeyringRow {
  const row = state.storage.sql
    .exec<KeyringRow>(
      "SELECT previous_kid, current_kid, next_kid, generation FROM jwk_keyring WHERE singleton = 1",
    )
    .toArray()[0];
  if (!row) throw new Error("missing keyring");
  return row;
}

function jwkKid(key: { kid?: string }): string | undefined {
  return key.kid;
}

describe("DOJWKStore", () => {
  it("initializes one current key and one pre-published next key", async () => {
    const stub = env.TEST.get(env.TEST.newUniqueId());
    await runInDurableObject(stub, async (_instance, state) => {
      const store = DOJWKStore.create(state);
      expect(DOJWKStore.create(state)).toBe(store);

      const lease = await store.getSigningKey();
      const jwks = await store.getJWKSet();
      expect(jwks.keys).toHaveLength(2);
      expect(jwks.keys.map(jwkKid)).toContain(lease.kid);
      expect(lease.privateKeyPkcs8.byteLength).toBeGreaterThan(0);
      expect(lease.signUntil).toBeGreaterThan(Date.now());
      const row = keyring(state);
      const keys = state.storage.sql
        .exec<{
          kid: string;
          created_at: number;
          ready_at: number;
          activated_at: number | null;
          sign_until: number | null;
          verify_until: number | null;
        }>(
          "SELECT kid, created_at, ready_at, activated_at, sign_until, verify_until FROM jwk_keys ORDER BY created_at",
        )
        .toArray();
      const current = keys.find((key) => key.kid === row.current_kid);
      const next = keys.find((key) => key.kid === row.next_kid);
      if (
        current == null ||
        next == null ||
        current.activated_at == null ||
        current.sign_until == null ||
        current.verify_until == null
      ) {
        throw new Error("missing initialized key timestamps");
      }
      expect(current).toMatchObject({
        kid: lease.kid,
        activated_at: expect.any(Number),
        sign_until: expect.any(Number),
        verify_until: expect.any(Number),
      });
      expect(current.sign_until - current.activated_at).toBe(KEY_SIGNING_LIFETIME);
      expect(current.verify_until - current.sign_until).toBe(KEY_VERIFY_GRACE);
      expect(next).toMatchObject({ activated_at: null, sign_until: null, verify_until: null });
      expect(next.ready_at - current.activated_at).toBe(JWKS_CACHE_TTL + CLOCK_SKEW);
      expect(
        state.storage.sql
          .exec<{ count: number }>("SELECT COUNT(*) AS count FROM jwk_keys")
          .toArray()[0]?.count,
      ).toBe(2);
    });
  });

  it("persists the keyring across adapter instances", async () => {
    const stub = env.TEST.get(env.TEST.newUniqueId());
    let kid = "";
    await runInDurableObject(stub, async (_instance, state) => {
      kid = (await DOJWKStore.create(state).getSigningKey()).kid;
    });
    await runInDurableObject(stub, async (_instance, state) => {
      expect((await DOJWKStore.create(state).getSigningKey()).kid).toBe(kid);
    });
  });

  it("keeps the current key until the staged next key is ready", async () => {
    const stub = env.TEST.get(env.TEST.newUniqueId());
    await runInDurableObject(stub, async (_instance, state) => {
      const store = DOJWKStore.create(state);
      const current = await store.getSigningKey();
      const staged = keyring(state).next_kid;

      // A rotation cannot use a key until it has spent the cache TTL in JWKS.
      state.storage.sql.exec("UPDATE jwk_keys SET sign_until = 0 WHERE kid = ?", current.kid);
      const future = Date.now() + JWKS_CACHE_TTL + CLOCK_SKEW;
      state.storage.sql.exec("UPDATE jwk_keys SET ready_at = ? WHERE kid = ?", future, staged);

      expect((await store.getSigningKey()).kid).toBe(current.kid);
      expect(keyring(state).generation).toBe(0);
    });
  });

  it("promotes the pre-published key after the signing window", async () => {
    const stub = env.TEST.get(env.TEST.newUniqueId());
    await runInDurableObject(stub, async (_instance, state) => {
      const store = DOJWKStore.create(state);
      const before = await store.getSigningKey();
      const nextKid = keyring(state).next_kid;
      expect(nextKid).toBeDefined();
      const expiredAt = Date.now() - 1;
      state.storage.sql.exec(
        "UPDATE jwk_keys SET sign_until = ?, verify_until = ? WHERE kid = ?",
        expiredAt,
        expiredAt + KEY_VERIFY_GRACE,
        before.kid,
      );
      state.storage.sql.exec("UPDATE jwk_keys SET ready_at = 0 WHERE kid = ?", nextKid);

      const after = await store.getSigningKey();
      expect(after.kid).toBe(nextKid);
      expect((await store.getJWKSet()).keys).toHaveLength(3);
      const rotated = keyring(state);
      const previous = state.storage.sql
        .exec<{ verify_until: number | null }>(
          "SELECT verify_until FROM jwk_keys WHERE kid = ?",
          rotated.previous_kid,
        )
        .toArray()[0];
      expect(previous?.verify_until).toBe(expiredAt + KEY_VERIFY_GRACE);
      expect(
        state.storage.sql
          .exec<{ generation: number }>("SELECT generation FROM jwk_keyring WHERE singleton = 1")
          .toArray()[0]?.generation,
      ).toBe(1);
    });
  });

  it("removes a previous key after its verification grace expires", async () => {
    const stub = env.TEST.get(env.TEST.newUniqueId());
    await runInDurableObject(stub, async (_instance, state) => {
      const store = DOJWKStore.create(state);
      const oldCurrent = await store.getSigningKey();
      const next = keyring(state).next_kid;
      state.storage.sql.exec("UPDATE jwk_keys SET sign_until = 0 WHERE kid = ?", oldCurrent.kid);
      state.storage.sql.exec("UPDATE jwk_keys SET ready_at = 0 WHERE kid = ?", next);
      await store.getSigningKey();
      expect(keyring(state).previous_kid).toBe(oldCurrent.kid);

      state.storage.sql.exec("UPDATE jwk_keys SET verify_until = 0 WHERE kid = ?", oldCurrent.kid);
      const jwks = await store.getJWKSet();
      expect(jwks.keys.map(jwkKid)).not.toContain(oldCurrent.kid);
      expect(keyring(state).previous_kid).toBeNull();
      expect(
        state.storage.sql
          .exec<{ count: number }>("SELECT COUNT(*) AS count FROM jwk_keys")
          .toArray()[0]?.count,
      ).toBe(2);
    });
  });

  it("serializes concurrent initialization and publishes one keyring", async () => {
    const stub = env.TEST.get(env.TEST.newUniqueId());
    await runInDurableObject(stub, async (_instance, state) => {
      const store = DOJWKStore.create(state);
      const leases = await Promise.all([
        store.getSigningKey(),
        store.getSigningKey(),
        store.getSigningKey(),
        store.getJWKSet().then(() => store.getSigningKey()),
      ]);
      expect(new Set(leases.map((lease) => lease.kid)).size).toBe(1);
      expect(keyring(state).generation).toBe(0);
      expect(
        state.storage.sql
          .exec<{ count: number }>("SELECT COUNT(*) AS count FROM jwk_keys")
          .toArray()[0]?.count,
      ).toBe(2);
    });
  });

  it("serializes concurrent rotation without republishing stale state", async () => {
    const stub = env.TEST.get(env.TEST.newUniqueId());
    await runInDurableObject(stub, async (_instance, state) => {
      const store = DOJWKStore.create(state);
      const before = await store.getSigningKey();
      const next = keyring(state).next_kid;
      state.storage.sql.exec("UPDATE jwk_keys SET sign_until = 0 WHERE kid = ?", before.kid);
      state.storage.sql.exec("UPDATE jwk_keys SET ready_at = 0 WHERE kid = ?", next);

      const leases = await Promise.all([
        store.getSigningKey(),
        store.getSigningKey(),
        store.getJWKSet().then(() => store.getSigningKey()),
        store.getJWKSet().then(() => store.getSigningKey()),
      ]);
      expect(new Set(leases.map((lease) => lease.kid)).size).toBe(1);
      expect(keyring(state).generation).toBe(1);
      expect(
        state.storage.sql
          .exec<{ count: number }>("SELECT COUNT(*) AS count FROM jwk_keys")
          .toArray()[0]?.count,
      ).toBe(3);
    });
  });
});
