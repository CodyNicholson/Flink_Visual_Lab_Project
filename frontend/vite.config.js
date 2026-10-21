import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

// REST calls and the SSE telemetry stream go through the dev server, so the browser never
// needs to know where the backend lives (http://backend:8080 inside docker compose).
const apiTarget = process.env.API_PROXY_TARGET || 'http://localhost:8080';

export default defineConfig({
  plugins: [react()],
  server: {
    port: 3000,
    host: true,
    proxy: {
      '/api': {
        target: apiTarget,
        changeOrigin: true
      }
    }
  }
});
