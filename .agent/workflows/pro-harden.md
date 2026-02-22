---
description: Restore an existing project to the Pro-Level baseline. Fixes broken symlinks, resets toxic configs, and ensures dependency integrity.
---

# Pro Harden (Repair Drift)

Run this workflow if you detect "Workspace Drift" or errors in your environment.

## 1. Detection

// turbo

```bash
dx
```

## 2. Force Restoration

// turbo

```bash
ops-heal
```

## 3. Surface Audit

Verify that no toxic paths remain in the core project configurations.
// turbo

```bash
grep -rEi "C:\\\\\\\\" . --exclude-dir=.venv --exclude-dir=.git
```

## 4. Certification

// turbo

```bash
dx
echo "✅ Environment Hardened. Drift Eliminated."
```
