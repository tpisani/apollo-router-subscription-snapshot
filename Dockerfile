FROM node:24-bookworm-slim
WORKDIR /app
COPY package*.json ./
RUN npm ci --omit=dev --ignore-scripts --no-audit --no-fund
COPY src ./src
COPY scripts ./scripts
COPY gateway/gateway.config.mjs ./gateway/
EXPOSE 4000 4001 4002
CMD ["node", "src/server.mjs"]
