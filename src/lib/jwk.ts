import { RpcTarget } from "cloudflare:workers";
import * as jose from "jose";

/** The amount of time for which a key may be used to sign tokens. */
export const KEY_SIGNING_LIFETIME = 3 * 60 * 60 * 1000;
/** Grafana's default JWKS cache lifetime. */
export const JWKS_CACHE_TTL = 60 * 60 * 1000;
/** The lifetime of tokens issued by the Grafana auth middleware. */
export const JWT_LIFETIME = 15 * 60 * 1000;
/** Allowance for clocks at either end of token verification. */
export const CLOCK_SKEW = 5 * 60 * 1000;

const KEY_READY_LEAD_TIME = JWKS_CACHE_TTL + CLOCK_SKEW;
const KEY_VERIFY_GRACE = JWT_LIFETIME + CLOCK_SKEW;

export interface SigningKeyLease {
  kid: string;
  privateKeyPkcs8: ArrayBuffer;
  signUntil: number;
}

interface StoredKey {
  kid: string;
  privateKeyPkcs8: ArrayBuffer;
  publicJwk: jose.JWK;
  createdAt: number;
  readyAt: number;
  activatedAt: number | null;
  signUntil: number | null;
  verifyUntil: number | null;
}

interface Keyring {
  previousKid: string | null;
  currentKid: string;
  nextKid: string;
  generation: number;
}

interface StoredKeyRow {
  [key: string]: SqlStorageValue;
  kid: string;
  private_key_pkcs8: ArrayBuffer;
  public_jwk: string;
  created_at: number;
  ready_at: number;
  activated_at: number | null;
  sign_until: number | null;
  verify_until: number | null;
}

interface KeyringRow {
  [key: string]: SqlStorageValue;
  previous_kid: string | null;
  current_kid: string;
  next_kid: string;
  generation: number;
}

/**
 * The key store used by one Durable Object instance.
 *
 * The private constructor and the WeakMap factory are intentional: a DO may
 * hand out this adapter from more than one RPC method, and all of those calls
 * must share the same Durable Object storage context.
 */
export class DOJWKStore extends RpcTarget {
  static readonly #instances = new WeakMap<DurableObjectState, DOJWKStore>();

  static readonly MIGRATIONS: ReadonlyArray<{
    version: number;
    up: (sql: SqlStorage) => void;
  }> = [
    {
      version: 1,
      up: (sql) => {
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
      },
    },
  ];

  readonly #ctx: DurableObjectState;

  private constructor(ctx: DurableObjectState) {
    super();
    this.#ctx = ctx;
    DOJWKStore.#migrate(ctx);
  }

  static create(ctx: DurableObjectState): DOJWKStore {
    const existing = DOJWKStore.#instances.get(ctx);
    if (existing) return existing;
    const store = new DOJWKStore(ctx);
    DOJWKStore.#instances.set(ctx, store);
    return store;
  }

  async getSigningKey(): Promise<SigningKeyLease> {
    await this.#ensureKeyring();
    const keyring = this.#readKeyring();
    if (!keyring) throw new Error("JWK keyring was not initialized");
    const key = this.#readKey(keyring.currentKid);
    if (!key || key.signUntil == null) {
      throw new Error("JWK keyring has no usable current key");
    }
    return {
      kid: key.kid,
      privateKeyPkcs8: key.privateKeyPkcs8.slice(0),
      signUntil: key.signUntil,
    };
  }

  async getJWKSet(): Promise<jose.JSONWebKeySet> {
    await this.#ensureKeyring();
    const keyring = this.#readKeyring();
    if (!keyring) throw new Error("JWK keyring was not initialized");

    const now = Date.now();
    const keys = [keyring.previousKid, keyring.currentKid, keyring.nextKid]
      .filter((kid): kid is string => kid != null)
      .map((kid) => this.#readKey(kid))
      .filter((key): key is StoredKey => key != null)
      .filter((key) => key.verifyUntil == null || key.verifyUntil > now)
      .map((key) => key.publicJwk);
    return jose.createLocalJWKSet({ keys }).jwks();
  }

  async #ensureKeyring(): Promise<void> {
    const logs = await this.#ctx.storage.transaction(async () => {
      const logs: { event: string; data: Record<string, unknown> }[] = [];
      const now = Date.now();
      let keyring = this.#readKeyring();
      if (!keyring) {
        const current = await generateKey(
          now,
          now,
          now,
          now + KEY_SIGNING_LIFETIME,
          now + KEY_SIGNING_LIFETIME + KEY_VERIFY_GRACE,
        );
        const next = await generateKey(now, now + KEY_READY_LEAD_TIME, null, null, null);
        const sql = this.#ctx.storage.sql;
        for (const key of [current, next]) {
          sql.exec(
            "INSERT INTO jwk_keys (kid, private_key_pkcs8, public_jwk, created_at, ready_at, activated_at, sign_until, verify_until) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            key.kid,
            key.privateKeyPkcs8,
            JSON.stringify(key.publicJwk),
            key.createdAt,
            key.readyAt,
            key.activatedAt,
            key.signUntil,
            key.verifyUntil,
          );
        }
        sql.exec(
          "INSERT INTO jwk_keyring (singleton, previous_kid, current_kid, next_kid, generation) VALUES (1, NULL, ?, ?, 0)",
          current.kid,
          next.kid,
        );
        logs.push({
          event: "jwk.keyring.initialized",
          data: { currentKid: current.kid, nextKid: next.kid, nextReadyAt: next.readyAt },
        });
        return logs;
      }

      const previous = keyring.previousKid ? this.#readKey(keyring.previousKid) : null;
      if (previous?.verifyUntil != null && previous.verifyUntil <= now) {
        const sql = this.#ctx.storage.sql;
        sql.exec("UPDATE jwk_keyring SET previous_kid = NULL WHERE singleton = 1");
        sql.exec("DELETE FROM jwk_keys WHERE kid = ?", previous.kid);
        logs.push({
          event: "jwk.keyring.previous_removed",
          data: {
            generation: keyring.generation,
            kid: previous.kid,
            verifyUntil: previous.verifyUntil,
          },
        });
        keyring = this.#readKeyring();
        if (!keyring) throw new Error("JWK keyring disappeared during cleanup");
      }

      const current = this.#readKey(keyring.currentKid);
      const next = this.#readKey(keyring.nextKid);
      if (!current || !next || current.signUntil == null) {
        throw new Error("JWK keyring is corrupt");
      }
      if (now < current.signUntil || now < next.readyAt) return logs;

      const newNext = await generateKey(now, now + KEY_READY_LEAD_TIME, null, null, null);
      const sql = this.#ctx.storage.sql;
      sql.exec(
        "INSERT INTO jwk_keys (kid, private_key_pkcs8, public_jwk, created_at, ready_at, activated_at, sign_until, verify_until) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        newNext.kid,
        newNext.privateKeyPkcs8,
        JSON.stringify(newNext.publicJwk),
        newNext.createdAt,
        newNext.readyAt,
        newNext.activatedAt,
        newNext.signUntil,
        newNext.verifyUntil,
      );
      sql.exec(
        "UPDATE jwk_keys SET activated_at = ?, sign_until = ?, verify_until = ? WHERE kid = ?",
        now,
        now + KEY_SIGNING_LIFETIME,
        now + KEY_SIGNING_LIFETIME + KEY_VERIFY_GRACE,
        next.kid,
      );
      sql.exec(
        "UPDATE jwk_keyring SET previous_kid = ?, current_kid = ?, next_kid = ?, generation = generation + 1 WHERE singleton = 1",
        current.kid,
        next.kid,
        newNext.kid,
      );
      logs.push({
        event: "jwk.keyring.rotated",
        data: {
          generation: keyring.generation + 1,
          previousKid: current.kid,
          currentKid: next.kid,
          nextKid: newNext.kid,
          nextReadyAt: newNext.readyAt,
        },
      });
      return logs;
    });
    for (const log of logs) {
      console.log(log.event, log.data);
    }
  }

  #readKeyring(): Keyring | null {
    const row = this.#ctx.storage.sql
      .exec<KeyringRow>(
        "SELECT previous_kid, current_kid, next_kid, generation FROM jwk_keyring WHERE singleton = 1",
      )
      .toArray()
      .at(0);
    if (!row) return null;
    return {
      previousKid: row.previous_kid,
      currentKid: row.current_kid,
      nextKid: row.next_kid,
      generation: row.generation,
    };
  }

  #readKey(kid: string): StoredKey | null {
    const row = this.#ctx.storage.sql
      .exec<StoredKeyRow>(
        "SELECT kid, private_key_pkcs8, public_jwk, created_at, ready_at, activated_at, sign_until, verify_until FROM jwk_keys WHERE kid = ?",
        kid,
      )
      .toArray()
      .at(0);
    if (!row) return null;
    return {
      kid: row.kid,
      privateKeyPkcs8: row.private_key_pkcs8,
      publicJwk: JSON.parse(row.public_jwk) as jose.JWK,
      createdAt: row.created_at,
      readyAt: row.ready_at,
      activatedAt: row.activated_at,
      signUntil: row.sign_until,
      verifyUntil: row.verify_until,
    };
  }

  static #migrate(ctx: DurableObjectState): void {
    const sql = ctx.storage.sql;
    sql.exec(`
      CREATE TABLE IF NOT EXISTS jwk_migrations (
        version INTEGER PRIMARY KEY,
        applied_at INTEGER NOT NULL
      )
    `);
    const currentVersion =
      sql
        .exec<{ version: number | null }>("SELECT MAX(version) AS version FROM jwk_migrations")
        .toArray()
        .at(0)?.version ?? 0;
    const migrations = DOJWKStore.MIGRATIONS.toSorted((a, b) => a.version - b.version);
    for (const migration of migrations) {
      if (migration.version <= currentVersion) continue;
      migration.up(sql);
      sql.exec(
        "INSERT INTO jwk_migrations (version, applied_at) VALUES (?, ?)",
        migration.version,
        Date.now(),
      );
    }
  }
}

async function generateKey(
  now: number,
  readyAt: number,
  activatedAt: number | null,
  signUntil: number | null,
  verifyUntil: number | null,
): Promise<StoredKey> {
  const pair = (await crypto.subtle.generateKey({ name: "Ed25519" }, true, [
    "sign",
    "verify",
  ])) as CryptoKeyPair;
  const privateKeyPkcs8 = (await crypto.subtle.exportKey("pkcs8", pair.privateKey)) as ArrayBuffer;
  const publicJwk = await jose.exportJWK(pair.publicKey);
  const kid = crypto.randomUUID();
  Object.assign(publicJwk, { alg: "EdDSA", use: "sig", kid });
  return {
    kid,
    privateKeyPkcs8,
    publicJwk,
    createdAt: now,
    readyAt,
    activatedAt,
    signUntil,
    verifyUntil,
  };
}
