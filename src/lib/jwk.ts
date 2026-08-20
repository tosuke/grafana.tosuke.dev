import { RpcTarget } from "cloudflare:workers";
import * as jose from "jose";

export interface SigningKey {
  kid: string;
  privateKeyPkcs8: ArrayBuffer;
}

interface StoredKey {
  kid: string;
  privateKeyPkcs8: ArrayBuffer;
  publicJwk: jose.JWK;
  createdAt: number;
}

interface StoredKeyRow {
  [key: string]: SqlStorageValue;
  kid: string;
  private_key_pkcs8: ArrayBuffer;
  public_jwk: string;
  created_at: number;
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
      version: 2,
      up: (sql) => {
        // Version 2 intentionally resets the old rotating keyring. The v2
        // JWKS URL makes Grafana fetch the replacement key immediately.
        sql.exec("DROP TABLE IF EXISTS jwk_keyring");
        sql.exec("DROP TABLE IF EXISTS jwk_keys");
        sql.exec(`
          CREATE TABLE jwk_keys (
            singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
            kid TEXT NOT NULL UNIQUE,
            private_key_pkcs8 BLOB NOT NULL,
            public_jwk TEXT NOT NULL,
            created_at INTEGER NOT NULL
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

  async getSigningKey(): Promise<SigningKey> {
    await this.#ensureKey();
    const key = this.#readKey();
    if (!key) throw new Error("JWK key was not initialized");
    return {
      kid: key.kid,
      privateKeyPkcs8: key.privateKeyPkcs8.slice(0),
    };
  }

  async getJWKSet(): Promise<jose.JSONWebKeySet> {
    await this.#ensureKey();
    const key = this.#readKey();
    if (!key) throw new Error("JWK key was not initialized");
    return jose.createLocalJWKSet({ keys: [key.publicJwk] }).jwks();
  }

  async #ensureKey(): Promise<void> {
    const initialized = await this.#ctx.storage.transaction(async () => {
      if (this.#readKey()) return false;

      const key = await generateKey(Date.now());
      this.#ctx.storage.sql.exec(
        "INSERT INTO jwk_keys (singleton, kid, private_key_pkcs8, public_jwk, created_at) VALUES (1, ?, ?, ?, ?)",
        key.kid,
        key.privateKeyPkcs8,
        JSON.stringify(key.publicJwk),
        key.createdAt,
      );
      return true;
    });
    if (initialized) {
      const key = this.#readKey();
      console.log("jwk.key.initialized", { kid: key?.kid, createdAt: key?.createdAt });
    }
  }

  #readKey(): StoredKey | null {
    const row = this.#ctx.storage.sql
      .exec<StoredKeyRow>(
        "SELECT kid, private_key_pkcs8, public_jwk, created_at FROM jwk_keys WHERE singleton = 1",
      )
      .toArray()
      .at(0);
    if (!row) return null;
    return {
      kid: row.kid,
      privateKeyPkcs8: row.private_key_pkcs8,
      publicJwk: JSON.parse(row.public_jwk) as jose.JWK,
      createdAt: row.created_at,
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

async function generateKey(now: number): Promise<StoredKey> {
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
  };
}
