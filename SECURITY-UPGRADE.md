# 0.6.22 security release candidate

This candidate raises cbor2 to >=5.9.0, msgpack to >=1.2.1 and multidict to
>=6.9.1,<7.1. It retains the 0.6.21 TLS disconnect fixes and existing LMDB
floor. Python 3.14 is not required: qualify deployment with Python 3.12.15.
The 3.12.2 CI job tests the compatibility promise only, not deployment safety.

## Trust boundary

HIO uses Python ssl and does not supply its own CA bundle. Updating this wheel
does not repair system trust, pip, other virtual environments or running SSL
contexts. Update OS CA packages and all retained packaging tools, rebuild
images without stale layers and restart processes. Bootstrap pip using the
hash-pinned command in .github/workflows/test.yaml after independently checking
[the Python announcement](https://discuss.python.org/t/information-for-python-users-regarding-emudhra-emsign-ca-certificate-security-incident/109576).
Repeat after creating each venv: ensurepip may contain an older pip wheel.

ClientTls creates a system-trusting context by default. cafilepath adds trust;
it does not remove public roots. Supplying a context alone is also insufficient:
without cafilepath, HIO loads default certificates when verification is enabled.
For private-only trust, construct SSLContext(PROTOCOL_TLS_CLIENT), retain
CERT_REQUIRED and check_hostname, and pass both that context and the private
CA cafilepath to ClientTls. Supply the expected certedhost. The server helper
likewise loads default certificates unless cafilepath is supplied. Never turn
off verification to work around an issuer or hostname failure.

## Qualification before release

The minimum and target constraints exercise security-sensitive dependencies;
they are not complete hashed deployment locks. CI preserves all three OSes.
Keep the downstream combined LMDB graph <=1.8.1 until separately qualified.

- [ ] Complete minimum/target OS and Python CI matrix.
- [ ] Verify valid chain/hostname success, wrong hostname and unrelated issuer
      rejection using actual HIO sockets, including private-only contexts.
- [ ] Verify incident chains are rejected by final runtime stores for issuer
      policy, with valid-chain positive controls; record CA and runtime inventory.
- [ ] Build wheel/sdist, inspect metadata, clean-install both and pip check.
- [ ] Qualify KERI 1.2.17 using an explicit temporary candidate dependency pin;
      do not describe --no-deps as a valid released dependency graph.
- [ ] Record complete dependency locks, hashes, advisory dispositions and
      downstream adoption receipts before publication and deployment closure.

This candidate does not change TLS trust semantics, publish artifacts, or
establish production remediation. The above unchecked gates block release.
