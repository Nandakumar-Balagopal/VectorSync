import { ClickableTile } from '@carbon/react';
import { Checkmark, WarningAlt, CircleFilled } from '@carbon/icons-react';
import type { SystemMetrics as SystemMetricsType } from '../types';
import './SystemMetrics.scss';

interface SystemMetricsProps {
  metrics: SystemMetricsType;
  loading: boolean;
}

export const SystemMetrics = ({ metrics, loading }: SystemMetricsProps) => {
  const estimatedCost = (metrics.totalVectors / 1000) * 0.12;
  const preventNavigation = (event: React.MouseEvent<HTMLAnchorElement>) => {
    event.preventDefault();
  };

  if (loading) {
    return (
      <div className="system-metrics">
        <ClickableTile className="metric-tile">Loading...</ClickableTile>
        <ClickableTile className="metric-tile">Loading...</ClickableTile>
        <ClickableTile className="metric-tile">Loading...</ClickableTile>
        <ClickableTile className="metric-tile">Loading...</ClickableTile>
      </div>
    );
  }

  const isActive = metrics.syncStatus === 'active';

  return (
    <div className="system-metrics">
      <ClickableTile href="#" onClick={preventNavigation} className="metric-tile">
        <div className="metric-header">Sync Status</div>
        <div className="metric-value">
          <CircleFilled
            size={16}
            className={`status-indicator ${isActive ? 'active' : 'inactive'}`}
          />
          <span className={`status-text ${isActive ? 'active' : 'inactive'}`}>
            {isActive ? 'Active' : 'Inactive'}
          </span>
        </div>
      </ClickableTile>

      <ClickableTile href="#" onClick={preventNavigation} className="metric-tile">
        <div className="metric-header">Vector Count</div>
        <div className="metric-value large">
          {metrics.totalVectors.toLocaleString()}
        </div>
        <div className="metric-sparkline">
          <svg viewBox="0 0 100 24" preserveAspectRatio="none">
            <path d="M0 18 L20 14 L38 16 L56 10 L75 8 L100 5" className="sparkline-path" />
          </svg>
        </div>
      </ClickableTile>

      <ClickableTile href="#" onClick={preventNavigation} className="metric-tile">
        <div className="metric-header">Latency</div>
        <div className="metric-value large">
          {metrics.avgSyncLatency.toFixed(1)}s
        </div>
        <div className="metric-wave">
          <svg viewBox="0 0 100 30" preserveAspectRatio="none">
            <path
              d="M0,15 Q25,10 50,15 T100,15"
              fill="none"
              className="sparkline-path"
              strokeWidth="2"
            />
          </svg>
        </div>
      </ClickableTile>

      <ClickableTile href="#" onClick={preventNavigation} className="metric-tile">
        <div className="metric-header">Cost (24h est.)</div>
        <div className="metric-value medium">${estimatedCost.toFixed(2)}</div>
        <div className="metric-subtext">
          Provider: {metrics.embeddingProvider}
        </div>
        <div className="metric-badge">
          {isActive ? <Checkmark size={16} /> : <WarningAlt size={16} />}
        </div>
      </ClickableTile>
    </div>
  );
};
