import { env } from "cloudflare:workers";
import type { MiddlewareHandler } from "hono";
import { createLocalJWKSet, exportJWK } from "jose";
import * as jose from "jose";

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

  const jwt = await new jose.SignJWT(payload)
    .setProtectedHeader({ alg: "EdDSA", kid: "1" })
    .setAudience(access.aud)
    .setIssuedAt()
    .setExpirationTime("1h")
    .sign(await privateKey());

  const headers = new Headers(c.req.raw.headers);
  headers.set("X-JWT-Assertion", jwt);
  c.req.raw = new Request(c.req.raw, { headers });

  await next();
};

export async function getJWKSet() {
  const pubKey = await publicKey();
  return createLocalJWKSet({
    keys: [
      {
        kid: "1",
        ...(await exportJWK(pubKey)),
      },
    ],
  });
}

let privateKeyCache: Promise<CryptoKey> | null = null;

function privateKey(): Promise<CryptoKey> {
  return (
    privateKeyCache ??
    (privateKeyCache = crypto.subtle.importKey(
      "pkcs8",
      Uint8Array.fromBase64(env.JWT_SIGN_KEY),
      "Ed25519",
      true,
      ["sign"],
    ))
  );
}

async function publicKey() {
  const privKey = await privateKey();
  const jwk = (await crypto.subtle.exportKey("jwk", privKey)) as JsonWebKey;
  const { d: _d, key_ops: _key_ops, ...publicJWK } = jwk;
  const publicKey = await crypto.subtle.importKey("jwk", publicJWK, "Ed25519", true, ["verify"]);
  return publicKey;
}
