import { describe, expect, it } from "vitest";
import { env } from "cloudflare:workers";
import { runInDurableObject } from "cloudflare:test";
import { DOJWKStore } from "./jwk";

describe("DOJWKStore", () => {
  it("migrates the rotating v1 schema to a fresh static v2 key", async () => {
    const stub = env.TEST.get(env.TEST.newUniqueId());
    await runInDurableObject(stub, async (_instance, state) => {
      const sql = state.storage.sql;
      sql.exec(`
        CREATE TABLE jwk_migrations (
          version INTEGER PRIMARY KEY,
          applied_at INTEGER NOT NULL
        )
      `);
      sql.exec("INSERT INTO jwk_migrations (version, applied_at) VALUES (1, 1)");
      sql.exec(`
        CREATE TABLE jwk_keys (
          kid TEXT PRIMARY KEY,
          private_key_pkcs8 BLOB NOT NULL,
          public_jwk TEXT NOT NULL,
          created_at INTEGER NOT NULL,
          ready_at INTEGER NOT NULL,
          activated_at INTEGER,
          sign_until INTEGER,
          verify_until INTEGER
        )
      `);
      sql.exec(`
        CREATE TABLE jwk_keyring (
          singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
          previous_kid TEXT REFERENCES jwk_keys(kid),
          current_kid TEXT NOT NULL REFERENCES jwk_keys(kid),
          next_kid TEXT NOT NULL REFERENCES jwk_keys(kid),
          generation INTEGER NOT NULL
        )
      `);
      sql.exec(
        "INSERT INTO jwk_keys (kid, private_key_pkcs8, public_jwk, created_at, ready_at, activated_at, sign_until, verify_until) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        "old-current",
        new ArrayBuffer(3),
        "{}",
        1,
        1,
        1,
        2,
        3,
      );
      sql.exec(
        "INSERT INTO jwk_keys (kid, private_key_pkcs8, public_jwk, created_at, ready_at, activated_at, sign_until, verify_until) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        "old-next",
        new ArrayBuffer(3),
        "{}",
        1,
        2,
        null,
        null,
        null,
      );
      sql.exec(
        "INSERT INTO jwk_keyring (singleton, previous_kid, current_kid, next_kid, generation) VALUES (1, NULL, ?, ?, 0)",
        "old-current",
        "old-next",
      );

      const store = DOJWKStore.create(state);
      expect(
        sql
          .exec<{ version: number }>("SELECT MAX(version) AS version FROM jwk_migrations")
          .toArray()[0]?.version,
      ).toBe(2);
      expect(
        sql
          .exec<{ count: number }>(
            "SELECT COUNT(*) AS count FROM sqlite_master WHERE name = 'jwk_keyring'",
          )
          .toArray()[0]?.count,
      ).toBe(0);

      const key = await store.getSigningKey();
      expect(key.kid).not.toBe("old-current");
      expect(
        sql.exec<{ count: number }>("SELECT COUNT(*) AS count FROM jwk_keys").toArray()[0]?.count,
      ).toBe(1);
      expect(
        sql.exec<{ kid: string }>("SELECT kid FROM jwk_keys WHERE singleton = 1").toArray()[0]?.kid,
      ).toBe(key.kid);
    });
  });

  it("lazily initializes one persistent key", async () => {
    const stub = env.TEST.get(env.TEST.newUniqueId());
    await runInDurableObject(stub, async (_instance, state) => {
      const store = DOJWKStore.create(state);
      expect(DOJWKStore.create(state)).toBe(store);

      const signingKey = await store.getSigningKey();
      const jwks = await store.getJWKSet();
      expect(jwks.keys).toHaveLength(1);
      expect(jwks.keys[0]).toMatchObject({ kid: signingKey.kid, alg: "EdDSA", use: "sig" });
      expect(signingKey.privateKeyPkcs8.byteLength).toBeGreaterThan(0);

      expect(
        state.storage.sql
          .exec<{ version: number }>("SELECT MAX(version) AS version FROM jwk_migrations")
          .toArray()[0]?.version,
      ).toBe(2);
      expect(
        state.storage.sql
          .exec<{ count: number }>("SELECT COUNT(*) AS count FROM jwk_keys")
          .toArray()[0]?.count,
      ).toBe(1);
      expect(
        state.storage.sql
          .exec<{ count: number }>(
            "SELECT COUNT(*) AS count FROM sqlite_master WHERE name = 'jwk_keyring'",
          )
          .toArray()[0]?.count,
      ).toBe(0);
    });
  });

  it("returns the same key across adapter instances", async () => {
    const stub = env.TEST.get(env.TEST.newUniqueId());
    let kid = "";
    await runInDurableObject(stub, async (_instance, state) => {
      kid = (await DOJWKStore.create(state).getSigningKey()).kid;
    });
    await runInDurableObject(stub, async (_instance, state) => {
      expect((await DOJWKStore.create(state).getSigningKey()).kid).toBe(kid);
      expect((await DOJWKStore.create(state).getJWKSet()).keys[0]?.kid).toBe(kid);
    });
  });

  it("serializes concurrent initialization", async () => {
    const stub = env.TEST.get(env.TEST.newUniqueId());
    await runInDurableObject(stub, async (_instance, state) => {
      const store = DOJWKStore.create(state);
      const keys = await Promise.all([
        store.getSigningKey(),
        store.getSigningKey(),
        store.getJWKSet().then(() => store.getSigningKey()),
        store.getJWKSet().then(() => store.getSigningKey()),
      ]);
      expect(new Set(keys.map((key) => key.kid)).size).toBe(1);
      expect(
        state.storage.sql
          .exec<{ count: number }>("SELECT COUNT(*) AS count FROM jwk_keys")
          .toArray()[0]?.count,
      ).toBe(1);
    });
  });

  it("creates a new key after the stored key is deleted", async () => {
    const stub = env.TEST.get(env.TEST.newUniqueId());
    await runInDurableObject(stub, async (_instance, state) => {
      const store = DOJWKStore.create(state);
      const oldKey = await store.getSigningKey();
      state.storage.sql.exec("DELETE FROM jwk_keys");

      const newKey = await store.getSigningKey();
      expect(newKey.kid).not.toBe(oldKey.kid);
      expect((await store.getJWKSet()).keys).toHaveLength(1);
      expect((await store.getJWKSet()).keys[0]?.kid).toBe(newKey.kid);
    });
  });
});
