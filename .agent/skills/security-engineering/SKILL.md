---
name: security-engineering
description: "Universal master security engineering, vulnerability auditing, and pentesting framework. Combines OWASP Top 10 mitigation, Insecure Direct Object Reference (IDOR/BOLA) prevention, SQL/Command injection elimination, SSRF protection, secret leakage detection, secure session and cookie hygiene, and automated security scanning."
allowed-tools: Read, Write, Edit, Run, Glob, Grep
version: 2.0.0
---

# Master Security Engineering & Vulnerability Auditing Framework

A comprehensive, unified discipline synthesizing application security, penetration testing tactics, vulnerability scanning, secure API design, cryptographic best practices, and zero-trust engineering.

---

## 1. Core Security Laws & Zero Trust Architecture

> [!IMPORTANT]
> **The Golden Law of Security:**
> **Never trust input from any boundary.** All input (query params, headers, request bodies, cookies, webhooks, external APIs) is potentially hostile. Validate strictly, fail closed, and enforce least privilege everywhere.

### Guiding Principles
1. **Least Privilege:** Every process, user, API key, and database role has the minimum permissions necessary to perform its specific duty.
2. **Fail Closed (Deny by Default):** If authorization check fails, times out, or throws an unhandled error, access is strictly denied.
3. **Defense-in-Depth:** Never rely on a single defensive layer (e.g. relying only on frontend validation or only on a network firewall).

---

## 2. OWASP Top 10 Vulnerabilities & Concrete Defenses

### A. Broken Access Control & IDOR / BOLA (Priority 1 Threat)
* **The Vulnerability:** `GET /api/documents/1042` returns document #1042 without verifying if the requesting user owns that document or belongs to that organization.
* **The Defense:**
  - Never trust client-supplied IDs without tenant/user ownership binding:
  ```python
  # BAD: Insecure Direct Object Reference
  doc = db.query(Document).filter(Document.id == doc_id).first()

  # SECURE: Enforce tenant / user ownership constraint
  doc = db.query(Document).filter(
      Document.id == doc_id,
      Document.tenant_id == current_user.tenant_id
  ).first_or_404()
  ```

### B. Injection Flaws (SQL, Command, Template, LDAP)
* **SQL Injection:**
  - ❌ NEVER interpolate strings into SQL: `f"SELECT * FROM users WHERE name = '{name}'"`
  - ✅ ALWAYS use parameterized queries / prepared statements: `cursor.execute("SELECT * FROM users WHERE name = %s", (name,))`
* **Command Injection:**
  - ❌ Avoid `child_process.exec()` or `os.system()` with shell expansion.
  - ✅ Use safe array execution (`subprocess.run(["cmd", arg1, arg2], shell=False)`).

### C. Server-Side Request Forgery (SSRF)
* **The Vulnerability:** An endpoint that fetches a URL supplied by a user (`GET /proxy?url=...`) can be manipulated to query internal cloud metadata services (`http://169.254.169.254/latest/meta-data/`) or private internal networks (`10.0.0.0/8`, `192.168.0.0/16`).
* **The Defense:**
  - Maintain a strict protocol and domain whitelist.
  - Reject private RFC1918 IPs, loopback (`127.0.0.1`), link-local (`169.254.169.254`), and IPv6 equivalents after DNS resolution.

### D. Cryptographic Failures & Secret Storage
* **Secrets Management:**
  - Zero hardcoded tokens or passwords in source code.
  - Store secrets in environment variables or KMS / HashiCorp Vault.
* **Password Hashing:**
  - Use memory-hard key derivation functions: **Argon2id** (recommended) or **bcrypt** (work factor $\ge 12$).
  - Never use MD5, SHA1, or plain SHA256 for passwords.

### E. Session Management & Cookie Hardening
* All authentication session cookies MUST carry the following flags:
  ```http
  Set-Cookie: session_id=xyz123; Secure; HttpOnly; SameSite=Strict; Path=/; Max-Age=3600
  ```
  - `Secure`: Transmitted only over HTTPS.
  - `HttpOnly`: Inaccessible to JavaScript (prevents token theft via XSS).
  - `SameSite=Strict`: Protects against Cross-Site Request Forgery (CSRF).

---

## 3. HTTP Security Headers Baseline

Ensure API gateways and web servers return the standard protective headers:

```http
Strict-Transport-Security: max-age=63072000; includeSubDomains; preload
X-Content-Type-Options: nosniff
X-Frame-Options: DENY
Content-Security-Policy: default-src 'self'; script-src 'self'; object-src 'none';
Referrer-Policy: strict-origin-when-cross-origin
Permissions-Policy: geolocation=(), camera=(), microphone=()
```

---

## 4. Automated Vulnerability Scanning & Auditing Commands

```bash
# 1. Dependency vulnerability audit
npm audit --audit-level=high
pip-audit --desc
cargo audit

# 2. Secret scanning in git history
gitleaks detect --verbose
trufflehog git file://. --since-commit HEAD~50

# 3. Static application security testing (SAST)
semgrep scan --config auto src/

# 4. Check for debug / administrative endpoints
curl -I http://localhost:8080/metrics
curl -I http://localhost:8080/actuator
curl -I http://localhost:8080/swagger-ui.html
```
