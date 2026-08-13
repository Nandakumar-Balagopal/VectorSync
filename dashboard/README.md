# VectorSync Dashboard

A professional React dashboard for managing and monitoring the VectorSync vector database synchronization system, built with IBM's Carbon Design System.

## Features

### 🎯 System Metrics
- **Real-time monitoring** of sync status, vector count, latency, and embedding provider
- **Visual indicators** with sparkline charts and status badges
- **Hover effects** for enhanced interactivity

### 📊 Vectorized Tables Management
- **DataTable view** of all registered tables with sorting and filtering
- **CRUD operations** for table configurations
- **Status tracking** (active/inactive) with visual tags
- **Actions menu** for edit and delete operations

### 🔍 Semantic Search Playground
- **Interactive search** interface with real-time results
- **Similarity scoring** with visual progress bars
- **Result cards** displaying matched content with metadata
- **Responsive design** with smooth animations

## Tech Stack

- **React 18** - Modern React with hooks
- **TypeScript** - Type-safe development
- **Carbon Design System** - IBM's enterprise design system
- **Vite** - Fast build tool and dev server
- **Axios** - HTTP client for API calls
- **SCSS** - Styling with Carbon's Gray 100 theme

## Getting Started

### Prerequisites

- Node.js 18+ and npm
- VectorSync backend services running (control-api, search-api, worker)

### Installation

```bash
cd dashboard
npm install
```

### Configuration

The dashboard connects to the VectorSync backend via proxy configuration in `vite.config.ts`:

```typescript
proxy: {
  '/api': {
    target: 'http://localhost:8080',
    changeOrigin: true,
  },
}
```

Ensure your backend services are running on the configured ports:
- Control API: `http://localhost:8081`
- Search API: `http://localhost:8082`
- Worker: `http://localhost:8083`

### Development

Start the development server:

```bash
npm run dev
```

The dashboard will be available at `http://localhost:5173`

### Build

Create a production build:

```bash
npm run build
```

The optimized build will be in the `dist/` directory.

### Preview Production Build

```bash
npm run preview
```

## Project Structure

```
dashboard/
├── public/
│   └── index.html          # HTML template
├── src/
│   ├── components/         # React components
│   │   ├── SystemMetrics.tsx
│   │   ├── SystemMetrics.scss
│   │   ├── VectorizedTables.tsx
│   │   ├── VectorizedTables.scss
│   │   ├── SemanticPlayground.tsx
│   │   └── SemanticPlayground.scss
│   ├── services/
│   │   └── api.ts          # API service layer
│   ├── types/
│   │   └── index.ts        # TypeScript type definitions
│   ├── App.tsx             # Main application component
│   ├── App.scss            # Global styles
│   ├── index.tsx           # Application entry point
│   └── index.scss          # Base styles
├── vite.config.ts          # Vite configuration
├── tsconfig.json           # TypeScript configuration
└── package.json            # Dependencies and scripts
```

## Design System

The dashboard follows IBM's Carbon Design System principles:

### Color Palette (Gray 100 Theme)
- **Background**: `#161616` (primary), `#262626` (secondary)
- **Borders**: `#393939`, `#525252`
- **Text**: `#f4f4f4` (primary), `#c6c6c6` (secondary), `#8d8d8d` (tertiary)
- **Accent**: `#0f62fe` (IBM Blue)
- **Success**: `#42be65` (Green)

### Typography
- **Font Family**: IBM Plex Sans
- **Weights**: 300 (Light), 400 (Regular), 500 (Medium), 600 (Semibold)
- **Letter Spacing**: Tight for headings (-0.02em to -0.03em)

### Spacing
- **Base unit**: 1rem (16px)
- **Component padding**: 1.5rem - 2rem
- **Grid gaps**: 1rem - 2rem

### Interactions
- **Transitions**: 0.2s ease for all interactive elements
- **Hover effects**: Subtle background changes and transforms
- **Focus states**: 2px solid `#0f62fe` outline

## API Integration

The dashboard communicates with three backend services:

### Control API (`/api/tables`)
- `GET /api/tables` - List all registered tables
- `POST /api/tables` - Register a new table
- `PUT /api/tables/{id}` - Update table configuration
- `DELETE /api/tables/{id}` - Delete a table

### Search API (`/api/search`)
- `POST /api/search` - Execute semantic search
  ```json
  {
    "query": "search text",
    "tableId": "uuid",
    "limit": 10
  }
  ```

### Worker API (`/api/sync`)
- `GET /api/sync/status` - Get sync status
- `POST /api/sync/trigger` - Trigger manual sync

## Component Documentation

### SystemMetrics
Displays real-time system health metrics in a 4-tile layout.

**Props**: None (fetches data internally)

**Features**:
- Auto-refresh every 30 seconds
- Sparkline charts for trend visualization
- Status indicators (active/inactive/error)
- Hover effects with border accents

### VectorizedTables
Manages registered tables with full CRUD operations.

**Props**: None (fetches data internally)

**Features**:
- Sortable DataTable with pagination
- Add/Edit/Delete operations
- Status tags (active/inactive)
- Actions menu per row
- Modal forms for create/edit

### SemanticPlayground
Interactive search interface for testing semantic queries.

**Props**: None (fetches data internally)

**Features**:
- Real-time search with debouncing
- Similarity score visualization
- Result cards with metadata
- Loading and empty states
- Responsive result list

## Customization

### Theming

To customize the theme, modify the Carbon theme import in `src/App.scss`:

```scss
@use '@carbon/react/scss/theme' with (
  $theme: themes.$g100  // Change to g10, g90, or white
);
```

### API Endpoints

Update the proxy configuration in `vite.config.ts` to point to your backend:

```typescript
proxy: {
  '/api': {
    target: 'http://your-backend-url',
    changeOrigin: true,
  },
}
```

### Styling

Component styles are modular and use SCSS. Each component has its own `.scss` file that can be customized independently.

## Performance Optimization

- **Code splitting**: Automatic with Vite
- **Tree shaking**: Removes unused Carbon components
- **Lazy loading**: Components load on demand
- **Memoization**: React.memo for expensive components
- **Debouncing**: Search input debounced to reduce API calls

## Browser Support

- Chrome/Edge (latest)
- Firefox (latest)
- Safari (latest)

## Troubleshooting

### CORS Issues
If you encounter CORS errors, ensure the Vite proxy is configured correctly and the backend allows requests from `http://localhost:5173`.

### Build Errors
Clear the cache and reinstall dependencies:
```bash
rm -rf node_modules package-lock.json
npm install
```

### Styling Issues
Ensure Carbon Design System styles are imported correctly in `src/index.scss`:
```scss
@use '@carbon/react';
```

## Contributing

1. Follow the existing code style and component structure
2. Use TypeScript for type safety
3. Follow Carbon Design System guidelines
4. Test all changes in development mode
5. Ensure production build succeeds

## License

This dashboard is part of the VectorSync project.

---

**Built with IBM Carbon Design System** | **Powered by React + Vite**
