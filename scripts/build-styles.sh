#!/usr/bin/env bash
# Compiles styles/app.scss against cdp-portal-frontend's Sass and GOV.UK Frontend,
# then copies the fonts. Run after the portal's styles or govuk-frontend change.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PORTAL_DIR="${PORTAL_DIR:-$ROOT/../cdp-portal-frontend}"
STATIC="$ROOT/app/static"
GOVUK_ASSETS="$PORTAL_DIR/node_modules/govuk-frontend/dist/govuk/assets"

"$PORTAL_DIR/node_modules/.bin/sass" \
  --load-path="$PORTAL_DIR/node_modules" \
  --load-path="$PORTAL_DIR/src/client/stylesheets" \
  --load-path="$PORTAL_DIR/src/server/common/components" \
  --pkg-importer=node \
  --quiet-deps \
  --no-source-map \
  --style=compressed \
  "$ROOT/styles/app.scss" "$STATIC/app.css"

# The portal's $govuk-assets-path points into its node_modules. Serve the fonts next to app.css instead.
sed -i -E 's#url\(("|\x27)?[^)"\x27]*govuk-frontend/dist/govuk/assets/#url(\1govuk/#g' "$STATIC/app.css"

mkdir -p "$STATIC/govuk/fonts"
cp "$GOVUK_ASSETS"/fonts/* "$STATIC/govuk/fonts/"

echo "govuk-frontend $(node -p "require('$PORTAL_DIR/node_modules/govuk-frontend/package.json').version")," \
  "cdp-portal-frontend $(git -C "$PORTAL_DIR" rev-parse --short HEAD)" > "$STATIC/VERSION"
