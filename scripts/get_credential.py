#!/usr/bin/env python3
"""
get_credential.py — Retrieve stored credentials from mempalace L2 vault.

Use: python3 scripts/get_credential.py <key>
Example: python3 scripts/get_credential.py credentials:github_oauth_token

Falls back to gh CLI token if key not found in L2.
"""

import sys
import json
import os
import subprocess

VAULT = os.path.expanduser('~/.mempalace/vault.jsonl')


def get_from_vault(key: str) -> str | None:
    if not os.path.exists(VAULT):
        return None
    with open(VAULT) as f:
        for line in f:
            try:
                entry = json.loads(line)
                if entry.get('key') == key:
                    return entry.get('value')
            except json.JSONDecodeError:
                continue
    return None


def get_github_token_via_gh() -> str | None:
    """Fallback: call gh auth token (reads from OS keyring)."""
    r = subprocess.run(['gh', 'auth', 'token'], capture_output=True, text=True)
    if r.returncode == 0:
        return r.stdout.strip()
    return None


def main():
    if len(sys.argv) < 2:
        print("Usage: get_credential.py <key> [--plain|--preview]")
        print("\nKnown keys:")
        print("  credentials:github_oauth_token")
        print("\nExamples:")
        print("  python3 scripts/get_credential.py credentials:github_oauth_token")
        print("  python3 scripts/get_credential.py credentials:github_oauth_token --preview")
        sys.exit(1)

    key = sys.argv[1]
    show_preview = '--preview' in sys.argv
    plain = '--plain' in sys.argv

    # 1 Try L2 mempalace vault
    value = get_from_vault(key)

    # 2 Fallback to gh CLI for GitHub token (if key not in vault)
    if value is None and 'github' in key.lower():
        value = get_github_token_via_gh()
        if value:
            print(f"[fallback] Retrieved via gh CLI", file=sys.stderr)

    if value is None:
        print(f"Error: key '{key}' not found in mempalace vault.", file=sys.stderr)
        sys.exit(1)

    if show_preview:
        print(f"Key: {key}")
        print(f"Value: {value[:20]}... (length {len(value)})")
    elif plain:
        print(value)  # raw for piping
    else:
        # default: show full value masked
        print(f"credentials:github_oauth_token = gh{value[2:22]}...{value[-6:]}")


if __name__ == '__main__':
    main()
