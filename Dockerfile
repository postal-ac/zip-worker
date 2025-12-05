# 1) Build stage
FROM node:20-alpine AS build

WORKDIR /app

# Only copy package files first (cache layer)
COPY package*.json ./

# Install deps
RUN npm ci

# Copy source
COPY tsconfig.json ./
COPY src ./src

# Build TypeScript → JS
RUN npm run build

# 2) Runtime stage (smaller)
FROM node:20-alpine AS runtime

WORKDIR /app

ENV NODE_ENV=production

# Copy only what we need to run
COPY package*.json ./
RUN npm ci --omit=dev

COPY --from=build /app/dist ./dist

# App listens on 4005 (match PORT in .env)
EXPOSE 4005

CMD ["node", "dist/server.js"]