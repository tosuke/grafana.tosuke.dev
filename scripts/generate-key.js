const key = await crypto.subtle.generateKey("Ed25519", true, ["sign", "verify"]);
const pkcsKey = await crypto.subtle.exportKey("pkcs8", key.privateKey);
process.stdout.write(new Uint8Array(pkcsKey).toBase64());
