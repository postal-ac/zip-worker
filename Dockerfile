# 1) Build stage: compile TypeScript and prepare prod node_modules
FROM node:20-alpine AS build

WORKDIR /app

# Install ALL deps (including dev) for building
COPY package*.json ./
RUN npm ci

# Copy source & config
COPY tsconfig.json ./
COPY src ./src

# Build TypeScript → JS
RUN npm run build

# Remove devDependencies so node_modules is production-only
RUN npm prune --omit=dev


# 2) Runtime stage: small image, prod deps only
FROM node:20-alpine AS runtime

WORKDIR /app

ENV NODE_ENV=production
# Hostinger will pass PORT, but we set a sane default
ENV PORT=4005

# Use non-root user for security (already exists in node image)
USER node

# Copy only what we need from build stage
COPY --chown=node:node --from=build /app/package*.json ./
COPY --chown=node:node --from=build /app/node_modules ./node_modules
COPY --chown=node:node --from=build /app/dist ./dist

# App listens on 4005 by default
EXPOSE 4005

CMD ["node", "dist/server.js"]