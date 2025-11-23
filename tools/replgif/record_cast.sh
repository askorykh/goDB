#!/usr/bin/env bash
set -euo pipefail

# Record a GoDB REPL session using asciinema.
# Usage: tools/replgif/record_cast.sh
# Produces: docs/repl-demo.cast

# Ensure asciinema is available
if ! command -v asciinema >/dev/null 2>&1; then
  echo "asciinema not found. Install with: pip install asciinema" >&2
  exit 1
fi

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

script_file=$(mktemp)
trap 'rm -f "$script_file"' EXIT

cat > "$script_file" <<'SCRIPT'
go run ./cmd/godb-server <<'SQL'
CREATE TABLE users (id INT, name STRING, active BOOL);
INSERT INTO users VALUES (1, 'Alice', true);
INSERT INTO users VALUES (2, 'Bob', false);
SELECT * FROM users;
UPDATE users SET active = true WHERE name = 'Bob';
SELECT name, active FROM users WHERE active = true;
DELETE FROM users WHERE id = 1;
SELECT * FROM users;
.exit
SQL
SCRIPT

asciinema rec -y -c "bash \"$script_file\"" docs/repl-demo.cast
