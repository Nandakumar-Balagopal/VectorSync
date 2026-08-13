# VectorSync Dashboard Setup Guide

## Prerequisites

Make sure you have Node.js and npm installed:
```bash
node --version  # Should be v16 or higher
npm --version   # Should be v8 or higher
```

If not installed, download from: https://nodejs.org/

## Step 1: Create React App with Vite (Modern & Fast)

Run this command in the project root directory:

```bash
npm create vite@latest dashboard -- --template react-ts
cd dashboard
```

## Step 2: Install Dependencies

Install all required packages:

```bash
npm install
npm install @carbon/react @carbon/icons-react axios sass
```

## Step 3: Configure Vite Proxy

Create/update `vite.config.ts` to proxy API calls to the backend:

```typescript
import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  server: {
    port: 3000,
    proxy: {
      '/api': {
        target: 'http://localhost:8080',
        changeOrigin: true
      }
    }
  }
})
```

## Step 5: Create Dashboard Files

After running the above commands, I'll create the following files for you:

### Core Files:
1. `src/App.tsx` - Main dashboard component
2. `src/App.scss` - Carbon Design System styles
3. `src/components/SystemMetrics.tsx` - System health metrics tiles
4. `src/components/VectorizedTables.tsx` - Table list with sync status
5. `src/components/SemanticPlayground.tsx` - Search interface
6. `src/services/api.ts` - API service layer
7. `src/types/index.ts` - TypeScript type definitions

## Step 6: Start the Dashboard

```bash
npm start
```

The dashboard will open at `http://localhost:3000`

## Step 7: Start Backend Services

In a separate terminal, start the VectorSync services:

```bash
cd ..  # Back to project root
docker-compose --profile demo up -d
```

## Complete Setup Commands

Here's the complete sequence to run:

```bash
# 1. Create Vite React app with TypeScript
npm create vite@latest dashboard -- --template react-ts

# 2. Navigate to dashboard
cd dashboard

# 3. Install all dependencies
npm install

# 4. Install Carbon Design System and other packages
npm install @carbon/react @carbon/icons-react axios sass

# 5. After I create the files, start the app
npm run dev
```

## Project Structure

After setup, your dashboard directory will look like:

```
dashboard/
├── package.json
├── tsconfig.json
├── public/
│   ├── index.html
│   └── favicon.ico
└── src/
    ├── App.tsx
    ├── App.scss
    ├── index.tsx
    ├── index.scss
    ├── components/
    │   ├── SystemMetrics.tsx
    │   ├── VectorizedTables.tsx
    │   └── SemanticPlayground.tsx
    ├── services/
    │   └── api.ts
    └── types/
        └── index.ts
```

## Dashboard Features

The dashboard will include:

1. **System Health Metrics**
   - Sync status (Active/Inactive)
   - Total vector count
   - Average sync latency
   - Embedding provider info

2. **Vectorized Tables**
   - List of registered tables
   - Sync status for each table
   - Last sync timestamp
   - Embedding model used
   - Actions (sync, disable, delete)

3. **Semantic Playground**
   - Search input
   - Real-time semantic search
   - Results with similarity scores
   - Source table information
   - Text snippets

## API Endpoints Used

The dashboard will connect to these endpoints:

- `GET /api/tables` - List all registered tables
- `GET /api/sync/status` - Get overall sync status
- `GET /api/vectors/count` - Get total vector count
- `POST /api/search` - Perform semantic search
- `POST /api/demo/sync` - Trigger manual sync

## Troubleshooting

### CORS Issues
If you see CORS errors, make sure the proxy configuration is in `vite.config.ts`.

### API Connection Issues
Ensure the backend services are running:
```bash
docker-compose ps
```

All services should show "Up" status.

### Port Already in Use
If port 3000 is in use:
```bash
# Kill the process using port 3000
lsof -ti:3000 | xargs kill -9

# Or use a different port by editing vite.config.ts
```

## Next Steps

After running the setup commands above, let me know and I'll create all the dashboard component files with the Carbon Design System implementation.