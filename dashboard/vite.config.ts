import { defineConfig } from 'vite'
import preact from '@preact/preset-vite'

export default defineConfig({
  base: '/dashboard/',
  plugins: [preact()],
  build: {
    outDir: 'dist',
    emptyOutDir: true,
  },
})
