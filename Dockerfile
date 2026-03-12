FROM node:22-bookworm-slim

RUN apt-get update && apt-get install -y \
    libglib2.0-0 libnss3 libatk1.0-0 libatk-bridge2.0-0 \
    libcups2 libxkbcommon0 libxcomposite1 libxdamage1 \
    libxfixes3 libxrandr2 libgbm1 libpango-1.0-0 \
    libcairo2 libasound2 libx11-6 libxext6 \
    && rm -rf /var/lib/apt/lists/*

RUN corepack enable pnpm

WORKDIR /app
COPY package.json pnpm-lock.yaml ./
RUN pnpm install --frozen-lockfile
RUN npx agent-browser install --with-deps

ENV AGENT_BROWSER_ARGS="--no-sandbox,--disable-dev-shm-usage"
ENV ANTHROPIC_API_KEY=""

COPY . .
RUN pnpm run build

CMD ["node", "dist/server.js"]
