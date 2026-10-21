# Production image: Vite builds a minified bundle, nginx serves it on port 80
# and proxies /api (REST + SSE) to the backend.
FROM node:20-alpine AS build
WORKDIR /app
COPY package.json ./
RUN npm install
COPY . .
# Vite inlines VITE_* variables at build time, so they come in as build args
ARG VITE_FLINK_EMBEDDED_UI_URL=http://localhost:8082
ARG VITE_FLINK_CLUSTER_UI_URL=http://localhost:8081
ENV VITE_FLINK_EMBEDDED_UI_URL=$VITE_FLINK_EMBEDDED_UI_URL \
    VITE_FLINK_CLUSTER_UI_URL=$VITE_FLINK_CLUSTER_UI_URL
RUN npm run build

FROM nginx:alpine
COPY --from=build /app/dist /usr/share/nginx/html
COPY nginx.conf /etc/nginx/conf.d/default.conf
EXPOSE 80
CMD ["nginx", "-g", "daemon off;"]
