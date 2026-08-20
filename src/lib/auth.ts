import type { MiddlewareHandler } from "hono";
import * as jose from "jose";
import { grafana } from "../grafana";

const JWT_LIFETIME = "15m";
const SIGNING_KEY_CACHE_TTL = 15 * 60 * 1000;

interface SigningKey {
  kid: string;
  privateKey: CryptoKey;
  loadedAt: number;
}

let signingKeyState: Promise<SigningKey> | null = null;

export const authMiddleware: MiddlewareHandler = async (c, next) => {
  const access = c.executionCtx.access;
  const identity = await access?.getIdentity();
  if (access == null || identity == null || identity.user_uuid == null) {
    return c.json({ error: "Cloudflare Access identity is required" }, 401);
  }

  const payload = {
    sub: identity.user_uuid,
    preferred_username: identity.name,
    email: identity.email,
    role: "GrafanaAdmin",
  } satisfies jose.JWTPayload;

  const key = await signingKey();
  const jwt = await new jose.SignJWT(payload)
    .setProtectedHeader({ alg: "EdDSA", kid: key.kid })
    .setAudience(access.aud)
    .setIssuedAt()
    .setExpirationTime(JWT_LIFETIME)
    .sign(key.privateKey);

  const headers = new Headers(c.req.raw.headers);
  headers.set("X-JWT-Assertion", jwt);
  c.req.raw = new Request(c.req.raw, { headers });

  await next();
};

async function signingKey(): Promise<SigningKey> {
  for (;;) {
    const state = signingKeyState;
    if (state != null) {
      try {
        const key = await state;
        if (Date.now() - key.loadedAt < SIGNING_KEY_CACHE_TTL) return key;
        if (signingKeyState === state) signingKeyState = null;
      } catch (error) {
        if (signingKeyState === state) signingKeyState = null;
        throw error;
      }
    }

    const pending = loadSigningKey();
    signingKeyState = pending;
    try {
      return await pending;
    } catch (error) {
      if (signingKeyState === pending) signingKeyState = null;
      throw error;
    }
  }
}

async function loadSigningKey(): Promise<SigningKey> {
  using stored = await grafana().jwkStore().getSigningKey();
  const privateKey = await crypto.subtle.importKey(
    "pkcs8",
    stored.privateKeyPkcs8,
    "Ed25519",
    false,
    ["sign"],
  );
  return {
    kid: stored.kid,
    privateKey,
    loadedAt: Date.now(),
  };
}
