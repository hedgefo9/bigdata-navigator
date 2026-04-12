#!/bin/sh
set -eu

export VAULT_ADDR="${VAULT_ADDR:-http://vault:8200}"
export VAULT_TOKEN="${VAULT_TOKEN:-root}"

until vault status >/dev/null 2>&1; do
  sleep 1
done

vault secrets enable -path=secret kv-v2 >/dev/null 2>&1 || true

vault kv put secret/bigdata-navigator/data-scout \
  postgres_password="navigator_password" >/dev/null

vault kv put secret/bigdata-navigator/generation \
  database_password="generation_password" \
  jwt_secret="change-me-in-prod" >/dev/null

vault kv put secret/bigdata-navigator/sources/catalog-postgres \
  password="data_password" >/dev/null

vault kv put secret/bigdata-navigator/sources/catalog-clickhouse \
  password="data_password" >/dev/null

echo "Vault bootstrap completed."
