import React, { useState, useEffect, useCallback, useRef } from 'react';
import {
  BarChart3,
  RefreshCw,
  Loader2,
  CheckCircle,
  XCircle,
  ChevronDown,
  ChevronRight,
  Clock,
  AlertCircle,
  Play
} from 'lucide-react';
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer
} from 'recharts';
import { API_BASE } from '../config';

const DIMENSIONS = ['completeness', 'uniqueness', 'validity', 'volume'];

const DIMENSION_COLORS = {
  completeness: { bg: 'bg-blue-50', border: 'border-blue-200', text: 'text-blue-700', icon: 'text-blue-600', bar: '#3b82f6' },
  uniqueness: { bg: 'bg-purple-50', border: 'border-purple-200', text: 'text-purple-700', icon: 'text-purple-600', bar: '#8b5cf6' },
  validity: { bg: 'bg-green-50', border: 'border-green-200', text: 'text-green-700', icon: 'text-green-600', bar: '#22c55e' },
  volume: { bg: 'bg-orange-50', border: 'border-orange-200', text: 'text-orange-700', icon: 'text-orange-600', bar: '#f97316' }
};

const METRIC_COLORS = [
  '#3b82f6', '#8b5cf6', '#22c55e', '#f97316', '#ef4444',
  '#06b6d4', '#ec4899', '#eab308', '#14b8a6', '#6366f1'
];

export default function ProfilingView({ connectionName, schemaName, tableName }) {
  const [profilingData, setProfilingData] = useState(null);
  const [runHistory, setRunHistory] = useState([]);
  const [isLoading, setIsLoading] = useState(false);
  const [isTriggering, setIsTriggering] = useState(false);
  const [triggerStatus, setTriggerStatus] = useState(null);
  const [activeDimension, setActiveDimension] = useState('completeness');
  const [historyExpanded, setHistoryExpanded] = useState(false);
  const [error, setError] = useState(null);
  const pollIntervalRef = useRef(null);

  const loadLatestResults = useCallback(async () => {
    setIsLoading(true);
    setError(null);
    try {
      const res = await fetch(
        `${API_BASE}/profiling/table/${encodeURIComponent(schemaName)}/${encodeURIComponent(tableName)}/latest`
      );
      if (res.ok) {
        const data = await res.json();
        setProfilingData(data);
      } else if (res.status === 404) {
        setProfilingData(null);
      } else {
        throw new Error('Failed to load profiling results');
      }
    } catch (err) {
      setError(err.message);
    } finally {
      setIsLoading(false);
    }
  }, [schemaName, tableName]);

  const loadHistory = useCallback(async () => {
    try {
      const res = await fetch(
        `${API_BASE}/profiling/table/${encodeURIComponent(schemaName)}/${encodeURIComponent(tableName)}/history`
      );
      if (res.ok) {
        const data = await res.json();
        setRunHistory(Array.isArray(data) ? data : data.runs || []);
      }
    } catch (err) {
      console.error('Failed to load history:', err);
    }
  }, [schemaName, tableName]);

  useEffect(() => {
    loadLatestResults();
    loadHistory();
    return () => {
      if (pollIntervalRef.current) clearInterval(pollIntervalRef.current);
    };
  }, [loadLatestResults, loadHistory]);

  const triggerProfiling = async () => {
    setIsTriggering(true);
    setTriggerStatus('running');
    setError(null);

    try {
      const res = await fetch(`${API_BASE}/profiling/trigger`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          connection_name: connectionName,
          tables: [{ schema_name: schemaName, table_name: tableName }],
          dimensions: DIMENSIONS
        })
      });

      if (!res.ok) {
        const errData = await res.json();
        throw new Error(errData.detail || 'Failed to trigger profiling');
      }

      const result = await res.json();
      const dagRunId = result.dag_run_id;
      const dagId = result.dag_id || 'dq_profiling';

      pollIntervalRef.current = setInterval(async () => {
        try {
          const statusRes = await fetch(
            `${API_BASE}/trigger/status/${encodeURIComponent(dagId)}/${encodeURIComponent(dagRunId)}`
          );
          if (!statusRes.ok) return;

          const status = await statusRes.json();

          if (status.state === 'success') {
            clearInterval(pollIntervalRef.current);
            pollIntervalRef.current = null;
            setTriggerStatus('success');
            setIsTriggering(false);
            await loadLatestResults();
            await loadHistory();
          } else if (status.state === 'failed') {
            clearInterval(pollIntervalRef.current);
            pollIntervalRef.current = null;
            setTriggerStatus('error');
            setIsTriggering(false);
            setError('Profiling job failed. Check Airflow logs.');
          }
        } catch {
          // ignore transient poll errors
        }
      }, 3000);
    } catch (err) {
      setTriggerStatus('error');
      setIsTriggering(false);
      setError(err.message);
    }
  };

  const groupByDimension = (results) => {
    if (!results) return {};
    const items = Array.isArray(results) ? results : results.results || [];
    const grouped = {};
    items.forEach((r) => {
      const dim = (r.dimension || 'other').toLowerCase();
      if (!grouped[dim]) grouped[dim] = [];
      grouped[dim].push(r);
    });
    return grouped;
  };

  const grouped = groupByDimension(profilingData);

  const getChartData = (dimensionResults) => {
    if (!dimensionResults) return [];
    const byColumn = {};
    dimensionResults.forEach((r) => {
      const col = r.column_name || '_table_';
      if (!byColumn[col]) byColumn[col] = { column: col };
      byColumn[col][r.metric_name] = r.actual_value;
    });
    return Object.values(byColumn);
  };

  const getMetricNames = (dimensionResults) => {
    if (!dimensionResults) return [];
    return [...new Set(dimensionResults.map((r) => r.metric_name))];
  };

  const dimensionResults = grouped[activeDimension] || [];
  const chartData = getChartData(dimensionResults);
  const metricNames = getMetricNames(dimensionResults);

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="bg-white rounded-lg shadow-sm border border-slate-200 p-6">
        <div className="flex items-center justify-between">
          <div className="flex items-center space-x-4">
            <div className="p-3 bg-indigo-100 rounded-lg">
              <BarChart3 className="w-8 h-8 text-indigo-600" />
            </div>
            <div>
              <h2 className="text-2xl font-bold text-slate-900">
                {schemaName}.{tableName}
              </h2>
              <p className="text-slate-500">Data Quality Profiling</p>
            </div>
          </div>

          <button
            onClick={triggerProfiling}
            disabled={isTriggering}
            className="flex items-center space-x-2 px-5 py-2.5 bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:bg-slate-400 disabled:cursor-not-allowed transition-colors font-medium"
          >
            {isTriggering ? (
              <>
                <Loader2 className="w-4 h-4 animate-spin" />
                <span>Profiling in progress...</span>
              </>
            ) : (
              <>
                <Play className="w-4 h-4" />
                <span>Trigger Profiling</span>
              </>
            )}
          </button>
        </div>

        {/* Trigger status banner */}
        {triggerStatus === 'success' && (
          <div className="mt-4 flex items-center space-x-2 text-green-700 bg-green-50 border border-green-200 rounded-lg px-4 py-2">
            <CheckCircle className="w-4 h-4" />
            <span className="text-sm font-medium">Profiling completed successfully</span>
          </div>
        )}
        {triggerStatus === 'error' && error && (
          <div className="mt-4 flex items-center space-x-2 text-red-700 bg-red-50 border border-red-200 rounded-lg px-4 py-2">
            <XCircle className="w-4 h-4" />
            <span className="text-sm font-medium">{error}</span>
          </div>
        )}
      </div>

      {/* Loading */}
      {isLoading && (
        <div className="flex items-center justify-center py-12">
          <Loader2 className="w-8 h-8 text-blue-600 animate-spin" />
        </div>
      )}

      {/* No data */}
      {!isLoading && !profilingData && !error && (
        <div className="bg-white rounded-lg shadow-sm border border-slate-200 p-12 text-center">
          <BarChart3 className="w-16 h-16 text-slate-300 mx-auto mb-4" />
          <h3 className="text-lg font-semibold text-slate-700 mb-2">No profiling data yet</h3>
          <p className="text-slate-500">
            Click "Trigger Profiling" to run data quality profiling on this table.
          </p>
        </div>
      )}

      {/* Results */}
      {!isLoading && profilingData && (
        <>
          {/* Summary Cards */}
          <div className="grid grid-cols-4 gap-4">
            {DIMENSIONS.map((dim) => {
              const count = (grouped[dim] || []).length;
              const colors = DIMENSION_COLORS[dim];
              return (
                <div
                  key={dim}
                  className={`${colors.bg} ${colors.border} border rounded-lg p-4 cursor-pointer hover:shadow-sm transition-shadow ${
                    activeDimension === dim ? 'ring-2 ring-offset-1 ring-blue-400' : ''
                  }`}
                  onClick={() => setActiveDimension(dim)}
                >
                  <p className={`text-sm font-medium ${colors.text} capitalize`}>{dim}</p>
                  <p className="text-2xl font-bold text-slate-900 mt-1">{count}</p>
                  <p className="text-xs text-slate-500">metrics</p>
                </div>
              );
            })}
          </div>

          {/* Dimension Tabs + Chart + Table */}
          <div className="bg-white rounded-lg shadow-sm border border-slate-200 overflow-hidden">
            {/* Tabs */}
            <div className="flex border-b border-slate-200">
              {DIMENSIONS.map((dim) => (
                <button
                  key={dim}
                  onClick={() => setActiveDimension(dim)}
                  className={`px-6 py-3 text-sm font-medium capitalize transition-colors ${
                    activeDimension === dim
                      ? 'text-blue-600 border-b-2 border-blue-600 bg-blue-50'
                      : 'text-slate-600 hover:text-slate-900 hover:bg-slate-50'
                  }`}
                >
                  {dim}
                  <span className="ml-2 text-xs bg-slate-100 text-slate-600 px-1.5 py-0.5 rounded-full">
                    {(grouped[dim] || []).length}
                  </span>
                </button>
              ))}
            </div>

            {/* Chart */}
            {chartData.length > 0 && (
              <div className="p-6 border-b border-slate-200">
                <h3 className="text-sm font-semibold text-slate-700 mb-4 uppercase tracking-wide">
                  {activeDimension} Metrics
                </h3>
                <ResponsiveContainer width="100%" height={300}>
                  <BarChart data={chartData} margin={{ top: 5, right: 30, left: 20, bottom: 5 }}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                    <XAxis
                      dataKey="column"
                      tick={{ fontSize: 12 }}
                      angle={-30}
                      textAnchor="end"
                      height={60}
                    />
                    <YAxis tick={{ fontSize: 12 }} />
                    <Tooltip
                      contentStyle={{
                        backgroundColor: '#fff',
                        border: '1px solid #e2e8f0',
                        borderRadius: '8px',
                        fontSize: '12px'
                      }}
                    />
                    <Legend wrapperStyle={{ fontSize: '12px' }} />
                    {metricNames.map((metric, idx) => (
                      <Bar
                        key={metric}
                        dataKey={metric}
                        fill={METRIC_COLORS[idx % METRIC_COLORS.length]}
                        radius={[4, 4, 0, 0]}
                      />
                    ))}
                  </BarChart>
                </ResponsiveContainer>
              </div>
            )}

            {/* Results Table */}
            <div className="p-6">
              <h3 className="text-sm font-semibold text-slate-700 mb-4 uppercase tracking-wide">
                Detailed Results
              </h3>
              {dimensionResults.length === 0 ? (
                <p className="text-sm text-slate-500 text-center py-8">
                  No metrics for this dimension
                </p>
              ) : (
                <div className="overflow-x-auto">
                  <table className="w-full text-sm">
                    <thead className="bg-slate-50">
                      <tr>
                        <th className="px-4 py-3 text-left font-medium text-slate-700">Column</th>
                        <th className="px-4 py-3 text-left font-medium text-slate-700">Metric</th>
                        <th className="px-4 py-3 text-right font-medium text-slate-700">Value</th>
                        <th className="px-4 py-3 text-left font-medium text-slate-700">Type</th>
                        <th className="px-4 py-3 text-right font-medium text-slate-700">Time (ms)</th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-slate-200">
                      {dimensionResults.map((r, idx) => (
                        <tr key={idx} className="hover:bg-slate-50">
                          <td className="px-4 py-3 font-medium text-slate-900">
                            {r.column_name || '-'}
                          </td>
                          <td className="px-4 py-3 text-slate-600">{r.metric_name}</td>
                          <td className="px-4 py-3 text-right font-mono text-slate-900">
                            {r.actual_value != null
                              ? typeof r.actual_value === 'number'
                                ? r.actual_value.toLocaleString(undefined, {
                                    maximumFractionDigits: 4
                                  })
                                : String(r.actual_value)
                              : '-'}
                          </td>
                          <td className="px-4 py-3 text-slate-500 text-xs">
                            {r.column_type || '-'}
                          </td>
                          <td className="px-4 py-3 text-right text-slate-500">
                            {r.execution_time_ms != null ? r.execution_time_ms : '-'}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </div>
          </div>

          {/* Run History */}
          {runHistory.length > 0 && (
            <div className="bg-white rounded-lg shadow-sm border border-slate-200 overflow-hidden">
              <button
                onClick={() => setHistoryExpanded(!historyExpanded)}
                className="w-full flex items-center justify-between px-6 py-4 hover:bg-slate-50 transition-colors"
              >
                <div className="flex items-center space-x-2">
                  <Clock className="w-5 h-5 text-slate-600" />
                  <h3 className="text-sm font-semibold text-slate-700 uppercase tracking-wide">
                    Run History
                  </h3>
                  <span className="text-xs bg-slate-100 text-slate-600 px-2 py-0.5 rounded-full">
                    {runHistory.length}
                  </span>
                </div>
                {historyExpanded ? (
                  <ChevronDown className="w-4 h-4 text-slate-400" />
                ) : (
                  <ChevronRight className="w-4 h-4 text-slate-400" />
                )}
              </button>

              {historyExpanded && (
                <div className="border-t border-slate-200">
                  <div className="overflow-x-auto">
                    <table className="w-full text-sm">
                      <thead className="bg-slate-50">
                        <tr>
                          <th className="px-4 py-3 text-left font-medium text-slate-700">
                            Run ID
                          </th>
                          <th className="px-4 py-3 text-left font-medium text-slate-700">
                            Started At
                          </th>
                          <th className="px-4 py-3 text-right font-medium text-slate-700">
                            Total Metrics
                          </th>
                          <th className="px-4 py-3 text-right font-medium text-slate-700">
                            Errors
                          </th>
                        </tr>
                      </thead>
                      <tbody className="divide-y divide-slate-200">
                        {runHistory.map((run, idx) => (
                          <tr key={idx} className="hover:bg-slate-50">
                            <td className="px-4 py-3 font-mono text-xs text-slate-600">
                              {run.profile_run_id || run.run_id || '-'}
                            </td>
                            <td className="px-4 py-3 text-slate-600">
                              {run.created_at || run.started_at
                                ? new Date(
                                    run.created_at || run.started_at
                                  ).toLocaleString()
                                : '-'}
                            </td>
                            <td className="px-4 py-3 text-right text-slate-900 font-medium">
                              {run.total_metrics ?? run.metric_count ?? '-'}
                            </td>
                            <td className="px-4 py-3 text-right">
                              {(run.error_count ?? run.errors ?? 0) > 0 ? (
                                <span className="flex items-center justify-end space-x-1 text-red-600">
                                  <AlertCircle className="w-3 h-3" />
                                  <span>{run.error_count ?? run.errors}</span>
                                </span>
                              ) : (
                                <span className="text-green-600">0</span>
                              )}
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              )}
            </div>
          )}
        </>
      )}
    </div>
  );
}
