#!/bin/bash
set -e

# Export environment variables if needed
export NEXT_BLOG_DATA_PATH=/tmp

# Clean up potential artifacts from Vercel's default behavior
rm -rf .next
rm -rf node_modules
rm -f bun.lockb

# Install dependencies from the root of the monorepo
# Ensure devDependencies are installed even if NODE_ENV=production
# This is crucial for building packages with vite/turbo
NODE_ENV=development bun --cwd=../.. install

# Build all packages in the workspace
bun --cwd=../.. run build

# Prepare static assets for the dashboard
mkdir -p "public/api/next-blog/dashboard/static/"
# Copy static assets (ensure source exists)
if [ -d "../../packages/core/dist/nextjs/assets/@supergrowthai/next-blog-dashboard/static" ]; then
  cp -r ../../packages/core/dist/nextjs/assets/@supergrowthai/next-blog-dashboard/static/* ./public/api/next-blog/dashboard/static/
else
  echo "Warning: Dashboard static assets not found at ../../packages/core/dist/nextjs/assets/@supergrowthai/next-blog-dashboard/static"
fi

# Build plugins
bun build-plugins.ts

# Run Next.js build using the binary from the root workspace
../../node_modules/.bin/next build
