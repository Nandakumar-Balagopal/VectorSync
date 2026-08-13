# VectorSync Dashboard - Troubleshooting Guide

## Black Screen on Global Pipeline Page

If you're seeing a black screen when navigating to `/pipeline`, follow these debugging steps:

### Step 1: Check Browser Console

Open your browser's Developer Tools (F12 or Cmd+Option+I on Mac) and check the Console tab for errors.

**Common errors to look for:**
- Import errors (missing modules)
- Type errors (undefined properties)
- API errors (failed requests)
- React errors (component rendering issues)

### Step 2: Check Network Tab

In Developer Tools, go to the Network tab and refresh the page.

**Look for:**
- Failed API requests (red status codes)
- Missing static assets (CSS, JS files)
- CORS errors

### Step 3: Verify Component Import

Check that GlobalPipeline is properly imported in `App.tsx`:

```typescript
import { GlobalPipeline } from './pages/GlobalPipeline';

// In Routes:
<Route path="/pipeline" element={<GlobalPipeline />} />
```

### Step 4: Check for TypeScript Errors

Run TypeScript compiler to check for type errors:

```bash
cd dashboard
npx tsc --noEmit
```

### Step 5: Verify Mock Data

The GlobalPipeline page relies on three API methods. Check that they return proper data:

1. **getPipelineStats()** - Should return:
```typescript
{
  totalJobsQueued: number,
  activeWorkers: number,
  cpuUsage: number,
  memoryUsage: number,
  backlogSize: number,
  jobsInProgress: number,
  throughput: number,
  successRate: number
}
```

2. **getWorkers()** - Should return array:
```typescript
[{
  workerId: string,
  status: 'active' | 'idle' | 'offline',
  cpuUsage: number,
  memoryUsage: number,
  currentJobs: number,
  maxCapacity: number,
  uptime: number,
  lastHeartbeat: string
}]
```

3. **getPerformanceMetrics()** - Should return:
```typescript
{
  cdcLatency: { p50, p95, p99, avg },
  embeddingLatency: { p50, p95, p99, avg },
  indexLatency: { p50, p95, p99, avg },
  errorRates: { cdcErrors, embeddingErrors, indexErrors, totalErrors, errorRate }
}
```

### Step 6: Test API Methods Directly

Add this to your browser console to test the API:

```javascript
// Test getPipelineStats
fetch('/api/pipeline/stats')
  .then(r => r.json())
  .then(console.log)
  .catch(console.error);

// Test getWorkers
fetch('/api/workers')
  .then(r => r.json())
  .then(console.log)
  .catch(console.error);

// Test getPerformanceMetrics
fetch('/api/metrics/performance')
  .then(r => r.json())
  .then(console.log)
  .catch(console.error);
```

### Step 7: Check SCSS Compilation

Verify that the SCSS file is being compiled correctly:

```bash
# Check if GlobalPipeline.scss exists
ls -la dashboard/src/pages/GlobalPipeline.scss

# Check for SCSS syntax errors
# Vite should show these in the terminal
```

### Step 8: Simplify Component for Testing

Create a minimal version to isolate the issue. Replace the GlobalPipeline component temporarily:

```typescript
export const GlobalPipeline: React.FC = () => {
  return (
    <div style={{ padding: '20px', color: 'white' }}>
      <h1>Global Pipeline - Test</h1>
      <p>If you see this, the route is working!</p>
    </div>
  );
};
```

If this works, gradually add back features to find the problematic code.

### Step 9: Check React DevTools

Install React DevTools browser extension and check:
- Is the GlobalPipeline component mounting?
- What are the component's props and state?
- Are there any error boundaries catching errors?

### Step 10: Common Fixes

#### Fix 1: Clear Build Cache
```bash
cd dashboard
rm -rf node_modules/.vite
npm run dev
```

#### Fix 2: Reinstall Dependencies
```bash
cd dashboard
rm -rf node_modules package-lock.json
npm install
npm run dev
```

#### Fix 3: Check for Missing Dependencies
```bash
cd dashboard
npm list @carbon/react @carbon/icons-react react react-dom
```

#### Fix 4: Verify Vite Config
Check `vite.config.ts` for proper React plugin configuration:

```typescript
import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      '/api': {
        target: 'http://localhost:8080',
        changeOrigin: true
      }
    }
  }
})
```

---

## Specific Error Messages

### "Cannot read property 'map' of undefined"

**Cause**: The `workers` array is undefined or null.

**Fix**: Check that `getWorkers()` returns an array. The component already has a safety check:
```typescript
setWorkers(Array.isArray(workerData) ? workerData : []);
```

### "Cannot read property 'p50' of undefined"

**Cause**: The `metrics` object is missing latency data.

**Fix**: Ensure `getPerformanceMetrics()` returns complete data structure.

### "ProgressBar is not a component"

**Cause**: Carbon Design System component not imported correctly.

**Fix**: Check import statement:
```typescript
import { ProgressBar } from '@carbon/react';
```

### "Module not found: Can't resolve './GlobalPipeline.scss'"

**Cause**: SCSS file missing or path incorrect.

**Fix**: 
```bash
# Create the file if missing
touch dashboard/src/pages/GlobalPipeline.scss

# Or remove the import temporarily
// import './GlobalPipeline.scss';
```

---

## Debug Mode

Add console logs to track component lifecycle:

```typescript
export const GlobalPipeline: React.FC = () => {
  console.log('GlobalPipeline: Component rendering');
  
  const [loading, setLoading] = useState(true);
  const [stats, setStats] = useState<GlobalPipelineStats | null>(null);
  const [workers, setWorkers] = useState<WorkerInfo[]>([]);
  const [metrics, setMetrics] = useState<PerformanceMetrics | null>(null);

  useEffect(() => {
    console.log('GlobalPipeline: useEffect triggered');
    loadData();
    const interval = setInterval(loadData, 5000);
    return () => clearInterval(interval);
  }, []);

  const loadData = async () => {
    console.log('GlobalPipeline: Loading data...');
    try {
      const [pipelineStats, workerData, performanceData] = await Promise.all([
        vectorSyncApi.getPipelineStats(),
        vectorSyncApi.getWorkers(),
        vectorSyncApi.getPerformanceMetrics(),
      ]);
      
      console.log('GlobalPipeline: Data loaded', {
        pipelineStats,
        workerData,
        performanceData
      });
      
      setStats(pipelineStats);
      setWorkers(Array.isArray(workerData) ? workerData : []);
      setMetrics(performanceData);
    } catch (err) {
      console.error('GlobalPipeline: Failed to load data', err);
    } finally {
      setLoading(false);
      console.log('GlobalPipeline: Loading complete');
    }
  };

  console.log('GlobalPipeline: Current state', { loading, stats, workers, metrics });

  // ... rest of component
};
```

---

## Still Having Issues?

If none of the above fixes work:

1. **Check other pages**: Do other pages (Overview, Search, etc.) work?
2. **Browser compatibility**: Try a different browser (Chrome, Firefox, Safari)
3. **Clear browser cache**: Hard refresh (Cmd+Shift+R or Ctrl+Shift+R)
4. **Check for ad blockers**: Disable browser extensions temporarily
5. **Review recent changes**: What was the last thing that worked?

---

## Quick Test Checklist

- [ ] Browser console shows no errors
- [ ] Network tab shows no failed requests
- [ ] TypeScript compiles without errors (`npx tsc --noEmit`)
- [ ] All dependencies are installed (`npm list`)
- [ ] Vite dev server is running without errors
- [ ] Other pages work correctly
- [ ] GlobalPipeline.tsx file exists and is valid
- [ ] GlobalPipeline.scss file exists
- [ ] Route is configured in App.tsx
- [ ] Component is exported correctly
- [ ] Mock data returns expected structure

---

## Contact Information

If you're still stuck, provide:
1. Browser console errors (screenshot or text)
2. Network tab errors
3. TypeScript compilation errors
4. Vite terminal output
5. Browser and version
6. Operating system

---

*Last Updated: 2026-04-28*  
*Made with Bob - VectorSync Dashboard Troubleshooting*