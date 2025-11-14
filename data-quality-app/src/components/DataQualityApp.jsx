import React, { useState } from 'react';
import { Database, CheckCircle, AlertCircle, Settings, Play, FileText, Plus, Trash2, Eye, BarChart3, LineChart, PieChart, TrendingUp, Activity } from 'lucide-react';
import { LineChart as RechartsLine, BarChart as RechartsBar, PieChart as RechartsPie, Line, Bar, Pie, Cell, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer, Area, AreaChart } from 'recharts';

export default function DataQualityApp() {
  const [activeTab, setActiveTab] = useState('connection');
  const [dbConfig, setDbConfig] = useState({
    host: 'localhost',
    port: '5432',
    database: '',
    schema: 'public',
    user: '',
    password: '',
    tableName: ''
  });

  const [sensorConfig, setSensorConfig] = useState({
    sensorCategory: '',
    sensorType: '',
    ruleName: ''
  });

  const [context, setContext] = useState({
    targetTable: {
      schemaName: 'bronze',
      tableName: ''
    },
    table: {
      filter: '',
      eventTimestampColumn: '',
      ingestionTimestampColumn: ''
    },
    columnName: '',
    errorSampling: {
      samplesLimit: '5',
      totalSamplesLimit: '1000',
      idColumns: []
    },
    parameters: {
      filter: '',
      expectedValues: [],
      columns: [],
      sqlExpression: '',
      sqlCondition: '',
      minValue: '',
      maxValue: '',
      expectedValue: ''
    },
    dataGroupings: {},
    timeSeries: {
      mode: '',
      timestampColumn: '',
      timeGradient: ''
    },
    timeWindowFilter: {
      dailyPartitioningRecentDays: '',
      dailyPartitioningIncludeToday: false
    }
  });

  const [idColumnInput, setIdColumnInput] = useState('');
  const [results, setResults] = useState(null);
  const [qualityHistory, setQualityHistory] = useState([
    { date: '2025-11-01', score: 92, passed: 920, failed: 80 },
    { date: '2025-11-02', score: 94, passed: 940, failed: 60 },
    { date: '2025-11-03', score: 89, passed: 890, failed: 110 },
    { date: '2025-11-04', score: 95, passed: 950, failed: 50 },
    { date: '2025-11-05', score: 93, passed: 930, failed: 70 },
    { date: '2025-11-06', score: 96, passed: 960, failed: 40 },
    { date: '2025-11-07', score: 95, passed: 950, failed: 50 }
  ]);

  // Data Visualization States
  const [dataVizConfig, setDataVizConfig] = useState({
    columns: [],
    chartType: 'bar',
    xColumn: '',
    yColumn: '',
    groupByColumn: '',
    aggregation: 'count'
  });

  const [sampleData, setSampleData] = useState([
    { gender: 'M', count: 450, avg_items: 3.2 },
    { gender: 'F', count: 550, avg_items: 2.8 },
  ]);

  const [qualityVizConfig, setQualityVizConfig] = useState({
    metric: 'score',
    chartType: 'line',
    dateRange: '7days'
  });

  const sensorCategories = {
    table: {
      label: 'Table Level',
      sensors: [
        { 
          value: 'completeness', 
          label: 'Completeness',
          rules: ['row_count', 'row_count_anomaly', 'table_completeness_percent']
        },
        { 
          value: 'uniqueness', 
          label: 'Uniqueness',
          rules: ['duplicate_count', 'duplicate_percent', 'duplicate_record_count']
        },
        { 
          value: 'timeliness', 
          label: 'Timeliness',
          rules: ['data_freshness', 'data_ingestion_delay']
        },
        { 
          value: 'volume', 
          label: 'Volume',
          rules: ['row_count', 'row_count_change', 'row_count_change_percent']
        },
        { 
          value: 'custom_sql', 
          label: 'Custom SQL',
          rules: ['custom_query']
        }
      ]
    },
    column: {
      label: 'Column Level',
      sensors: [
        { 
          value: 'accepted_values', 
          label: 'Accepted Values',
          rules: ['expected_values_in_set', 'invalid_percent', 'valid_percent']
        },
        { 
          value: 'accuracy', 
          label: 'Accuracy',
          rules: ['average', 'between_floats', 'between_ints', 'between_percents', 'max_diff_to_expected_value', 'max_diff_percent_to_expected_value']
        },
        { 
          value: 'completeness', 
          label: 'Completeness',
          rules: ['null_count', 'null_percent', 'not_null_count', 'not_null_percent']
        },
        { 
          value: 'consistency', 
          label: 'Consistency',
          rules: ['between_change', 'between_change_percent', 'standard_deviation', 'sum_equals']
        },
        { 
          value: 'validity', 
          label: 'Validity',
          rules: ['invalid_email', 'text_length_range', 'regex_match', 'invalid_values_percent']
        },
        { 
          value: 'uniqueness', 
          label: 'Uniqueness',
          rules: ['distinct_count', 'distinct_percent', 'duplicate_count']
        }
      ]
    }
  };

  const ruleCategories = {
    average: ['average'],
    change: ['between_change', 'between_change_percent', 'row_count_change', 'row_count_change_percent'],
    comparison: ['between_floats', 'between_ints', 'between_percents', 'max_diff_to_expected_value', 'max_diff_percent_to_expected_value']
  };

  const chartTypes = [
    { value: 'bar', label: 'Bar Chart', icon: BarChart3 },
    { value: 'line', label: 'Line Chart', icon: LineChart },
    { value: 'area', label: 'Area Chart', icon: Activity },
    { value: 'pie', label: 'Pie Chart', icon: PieChart }
  ];

  const aggregations = [
    { value: 'count', label: 'Count' },
    { value: 'sum', label: 'Sum' },
    { value: 'avg', label: 'Average' },
    { value: 'min', label: 'Minimum' },
    { value: 'max', label: 'Maximum' }
  ];

  const COLORS = ['#3B82F6', '#10B981', '#F59E0B', '#EF4444', '#8B5CF6', '#EC4899'];

  const handleRunQualityCheck = () => {
    setTimeout(() => {
      const newResult = {
        status: 'success',
        totalRecords: 1000,
        passedRecords: 950,
        failedRecords: 50,
        qualityScore: 95,
        details: [
          { rule: sensorConfig.ruleName || 'test_rule', passed: 950, failed: 50, percentage: 95 }
        ]
      };
      setResults(newResult);
      
      // Add to history
      const newDate = new Date().toISOString().split('T')[0];
      setQualityHistory(prev => [...prev, {
        date: newDate,
        score: newResult.qualityScore,
        passed: newResult.passedRecords,
        failed: newResult.failedRecords
      }].slice(-30));
    }, 1500);
  };

  const addToArray = (field, value, setter) => {
    if (value.trim()) {
      setter(prev => ({
        ...prev,
        [field]: [...prev[field], value.trim()]
      }));
    }
  };

  const renderDataVisualization = () => {
    const chartData = sampleData;

    return (
      <div className="space-y-6">
        <h3 className="text-lg font-semibold text-slate-900">Data Visualization</h3>
        
        {/* Configuration Panel */}
        <div className="bg-slate-50 rounded-lg p-4 space-y-4">
          <div className="grid grid-cols-2 gap-4">
            <div>
              <label className="block text-sm font-medium text-slate-700 mb-2">Chart Type</label>
              <div className="grid grid-cols-2 gap-2">
                {chartTypes.map(type => (
                  <button
                    key={type.value}
                    onClick={() => setDataVizConfig({...dataVizConfig, chartType: type.value})}
                    className={`flex items-center space-x-2 px-4 py-2 rounded-lg border-2 transition-colors ${
                      dataVizConfig.chartType === type.value
                        ? 'border-blue-600 bg-blue-50 text-blue-700'
                        : 'border-slate-200 hover:border-slate-300'
                    }`}
                  >
                    <type.icon className="w-4 h-4" />
                    <span className="text-sm">{type.label}</span>
                  </button>
                ))}
              </div>
            </div>

            <div>
              <label className="block text-sm font-medium text-slate-700 mb-2">X-Axis Column</label>
              <select
                value={dataVizConfig.xColumn}
                onChange={(e) => setDataVizConfig({...dataVizConfig, xColumn: e.target.value})}
                className="w-full px-4 py-2 border border-slate-300 rounded-lg"
              >
                <option value="">Select column...</option>
                <option value="gender">gender</option>
                <option value="status">status</option>
                <option value="created_at">created_at</option>
              </select>
            </div>

            <div>
              <label className="block text-sm font-medium text-slate-700 mb-2">Y-Axis Column</label>
              <select
                value={dataVizConfig.yColumn}
                onChange={(e) => setDataVizConfig({...dataVizConfig, yColumn: e.target.value})}
                className="w-full px-4 py-2 border border-slate-300 rounded-lg"
              >
                <option value="">Select column...</option>
                <option value="count">count</option>
                <option value="avg_items">avg_items</option>
                <option value="num_of_item">num_of_item</option>
              </select>
            </div>

            <div>
              <label className="block text-sm font-medium text-slate-700 mb-2">Aggregation</label>
              <select
                value={dataVizConfig.aggregation}
                onChange={(e) => setDataVizConfig({...dataVizConfig, aggregation: e.target.value})}
                className="w-full px-4 py-2 border border-slate-300 rounded-lg"
              >
                {aggregations.map(agg => (
                  <option key={agg.value} value={agg.value}>{agg.label}</option>
                ))}
              </select>
            </div>
          </div>
        </div>

        {/* Chart Display */}
        <div className="bg-white border border-slate-200 rounded-lg p-6">
          <ResponsiveContainer width="100%" height={400}>
            {dataVizConfig.chartType === 'bar' && (
              <RechartsBar data={chartData}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="gender" />
                <YAxis />
                <Tooltip />
                <Legend />
                <Bar dataKey="count" fill="#3B82F6" />
                <Bar dataKey="avg_items" fill="#10B981" />
              </RechartsBar>
            )}
            
            {dataVizConfig.chartType === 'line' && (
              <RechartsLine data={chartData}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="gender" />
                <YAxis />
                <Tooltip />
                <Legend />
                <Line type="monotone" dataKey="count" stroke="#3B82F6" strokeWidth={2} />
                <Line type="monotone" dataKey="avg_items" stroke="#10B981" strokeWidth={2} />
              </RechartsLine>
            )}

            {dataVizConfig.chartType === 'area' && (
              <AreaChart data={chartData}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="gender" />
                <YAxis />
                <Tooltip />
                <Legend />
                <Area type="monotone" dataKey="count" stackId="1" stroke="#3B82F6" fill="#3B82F6" fillOpacity={0.6} />
                <Area type="monotone" dataKey="avg_items" stackId="2" stroke="#10B981" fill="#10B981" fillOpacity={0.6} />
              </AreaChart>
            )}

            {dataVizConfig.chartType === 'pie' && (
              <RechartsPie>
                <Pie
                  data={chartData}
                  cx="50%"
                  cy="50%"
                  labelLine={false}
                  label={({gender, count}) => `${gender}: ${count}`}
                  outerRadius={120}
                  fill="#8884d8"
                  dataKey="count"
                >
                  {chartData.map((entry, index) => (
                    <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                  ))}
                </Pie>
                <Tooltip />
                <Legend />
              </RechartsPie>
            )}
          </ResponsiveContainer>
        </div>

        {/* Sample Data Table */}
        <div className="bg-white border border-slate-200 rounded-lg overflow-hidden">
          <div className="px-6 py-4 bg-slate-50 border-b border-slate-200">
            <h4 className="font-medium text-slate-900">Sample Data Preview</h4>
          </div>
          <div className="overflow-x-auto">
            <table className="w-full">
              <thead className="bg-slate-50">
                <tr>
                  {Object.keys(chartData[0] || {}).map(key => (
                    <th key={key} className="px-6 py-3 text-left text-sm font-medium text-slate-700">
                      {key}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {chartData.map((row, idx) => (
                  <tr key={idx} className="border-t border-slate-200">
                    {Object.values(row).map((val, vidx) => (
                      <td key={vidx} className="px-6 py-4 text-sm text-slate-900">{val}</td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      </div>
    );
  };

  const renderQualityVisualization = () => {
    return (
      <div className="space-y-6">
        <h3 className="text-lg font-semibold text-slate-900">Quality Metrics Visualization</h3>
        
        {/* Configuration */}
        <div className="bg-slate-50 rounded-lg p-4">
          <div className="grid grid-cols-3 gap-4">
            <div>
              <label className="block text-sm font-medium text-slate-700 mb-2">Metric</label>
              <select
                value={qualityVizConfig.metric}
                onChange={(e) => setQualityVizConfig({...qualityVizConfig, metric: e.target.value})}
                className="w-full px-4 py-2 border border-slate-300 rounded-lg"
              >
                <option value="score">Quality Score</option>
                <option value="passed">Passed Records</option>
                <option value="failed">Failed Records</option>
              </select>
            </div>

            <div>
              <label className="block text-sm font-medium text-slate-700 mb-2">Chart Type</label>
              <select
                value={qualityVizConfig.chartType}
                onChange={(e) => setQualityVizConfig({...qualityVizConfig, chartType: e.target.value})}
                className="w-full px-4 py-2 border border-slate-300 rounded-lg"
              >
                <option value="line">Line Chart</option>
                <option value="area">Area Chart</option>
                <option value="bar">Bar Chart</option>
              </select>
            </div>

            <div>
              <label className="block text-sm font-medium text-slate-700 mb-2">Date Range</label>
              <select
                value={qualityVizConfig.dateRange}
                onChange={(e) => setQualityVizConfig({...qualityVizConfig, dateRange: e.target.value})}
                className="w-full px-4 py-2 border border-slate-300 rounded-lg"
              >
                <option value="7days">Last 7 Days</option>
                <option value="30days">Last 30 Days</option>
                <option value="90days">Last 90 Days</option>
              </select>
            </div>
          </div>
        </div>

        {/* Quality Trend Chart */}
        <div className="bg-white border border-slate-200 rounded-lg p-6">
          <h4 className="font-medium text-slate-900 mb-4">Quality Score Trend</h4>
          <ResponsiveContainer width="100%" height={350}>
            {qualityVizConfig.chartType === 'line' && (
              <RechartsLine data={qualityHistory}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="date" />
                <YAxis />
                <Tooltip />
                <Legend />
                <Line type="monotone" dataKey="score" stroke="#3B82F6" strokeWidth={2} name="Quality Score" />
              </RechartsLine>
            )}

            {qualityVizConfig.chartType === 'area' && (
              <AreaChart data={qualityHistory}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="date" />
                <YAxis />
                <Tooltip />
                <Legend />
                <Area type="monotone" dataKey="score" stroke="#3B82F6" fill="#3B82F6" fillOpacity={0.6} name="Quality Score" />
              </AreaChart>
            )}

            {qualityVizConfig.chartType === 'bar' && (
              <RechartsBar data={qualityHistory}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="date" />
                <YAxis />
                <Tooltip />
                <Legend />
                <Bar dataKey="score" fill="#3B82F6" name="Quality Score" />
              </RechartsBar>
            )}
          </ResponsiveContainer>
        </div>

        {/* Pass/Fail Comparison */}
        <div className="bg-white border border-slate-200 rounded-lg p-6">
          <h4 className="font-medium text-slate-900 mb-4">Pass vs Fail Trend</h4>
          <ResponsiveContainer width="100%" height={350}>
            <AreaChart data={qualityHistory}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="date" />
              <YAxis />
              <Tooltip />
              <Legend />
              <Area type="monotone" dataKey="passed" stackId="1" stroke="#10B981" fill="#10B981" fillOpacity={0.8} name="Passed" />
              <Area type="monotone" dataKey="failed" stackId="1" stroke="#EF4444" fill="#EF4444" fillOpacity={0.8} name="Failed" />
            </AreaChart>
          </ResponsiveContainer>
        </div>

        {/* Statistics Summary */}
        <div className="grid grid-cols-4 gap-4">
          <div className="bg-blue-50 border border-blue-200 rounded-lg p-4">
            <div className="text-sm font-medium text-blue-600 mb-1">Avg Score</div>
            <div className="text-2xl font-bold text-blue-900">
              {(qualityHistory.reduce((acc, curr) => acc + curr.score, 0) / qualityHistory.length).toFixed(1)}%
            </div>
          </div>
          <div className="bg-green-50 border border-green-200 rounded-lg p-4">
            <div className="text-sm font-medium text-green-600 mb-1">Best Score</div>
            <div className="text-2xl font-bold text-green-900">
              {Math.max(...qualityHistory.map(h => h.score))}%
            </div>
          </div>
          <div className="bg-red-50 border border-red-200 rounded-lg p-4">
            <div className="text-sm font-medium text-red-600 mb-1">Worst Score</div>
            <div className="text-2xl font-bold text-red-900">
              {Math.min(...qualityHistory.map(h => h.score))}%
            </div>
          </div>
          <div className="bg-purple-50 border border-purple-200 rounded-lg p-4">
            <div className="text-sm font-medium text-purple-600 mb-1">Trend</div>
            <div className="flex items-center space-x-1">
              <TrendingUp className="w-5 h-5 text-purple-900" />
              <div className="text-2xl font-bold text-purple-900">
                +{(qualityHistory[qualityHistory.length - 1].score - qualityHistory[0].score).toFixed(1)}%
              </div>
            </div>
          </div>
        </div>
      </div>
    );
  };

  return (
    <div className="min-h-screen bg-gradient-to-br from-slate-50 to-slate-100">
      {/* Header */}
      <div className="bg-white shadow-sm border-b border-slate-200">
        <div className="max-w-7xl mx-auto px-6 py-4">
          <div className="flex items-center justify-between">
            <div className="flex items-center space-x-3">
              <div className="bg-blue-600 p-2 rounded-lg">
                <Database className="w-6 h-6 text-white" />
              </div>
              <div>
                <h1 className="text-2xl font-bold text-slate-900">Data Quality Assessment</h1>
                <p className="text-sm text-slate-500">Validate and monitor your data quality</p>
              </div>
            </div>
            <button
              onClick={handleRunQualityCheck}
              disabled={!dbConfig.database || !sensorConfig.sensorType}
              className="flex items-center space-x-2 bg-blue-600 text-white px-6 py-2.5 rounded-lg hover:bg-blue-700 disabled:bg-slate-300 disabled:cursor-not-allowed transition-colors"
            >
              <Play className="w-4 h-4" />
              <span>Run Check</span>
            </button>
          </div>
        </div>
      </div>

      <div className="max-w-7xl mx-auto px-6 py-8">
        {/* Tab Navigation */}
        <div className="bg-white rounded-lg shadow-sm border border-slate-200 mb-6">
          <div className="flex border-b border-slate-200 overflow-x-auto">
            {[
              { id: 'connection', label: 'Database', icon: Database },
              { id: 'sensor', label: 'Sensor & Rules', icon: Settings },
              { id: 'context', label: 'Context', icon: FileText },
              { id: 'results', label: 'Results', icon: CheckCircle },
              { id: 'data-viz', label: 'Data Charts', icon: BarChart3 },
              { id: 'quality-viz', label: 'Quality Charts', icon: TrendingUp }
            ].map((tab) => (
              <button
                key={tab.id}
                onClick={() => setActiveTab(tab.id)}
                className={`flex items-center space-x-2 px-6 py-4 font-medium transition-colors whitespace-nowrap ${
                  activeTab === tab.id
                    ? 'text-blue-600 border-b-2 border-blue-600'
                    : 'text-slate-600 hover:text-slate-900'
                }`}
              >
                <tab.icon className="w-4 h-4" />
                <span>{tab.label}</span>
              </button>
            ))}
          </div>

          <div className="p-6">
            {/* Database Connection Tab */}
            {activeTab === 'connection' && (
              <div className="space-y-6">
                <h3 className="text-lg font-semibold text-slate-900 mb-4">PostgreSQL Connection</h3>
                
                <div className="grid grid-cols-2 gap-4">
                  <div>
                    <label className="block text-sm font-medium text-slate-700 mb-2">Host</label>
                    <input
                      type="text"
                      value={dbConfig.host}
                      onChange={(e) => setDbConfig({...dbConfig, host: e.target.value})}
                      className="w-full px-4 py-2 border border-slate-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                      placeholder="localhost"
                    />
                  </div>
                  
                  <div>
                    <label className="block text-sm font-medium text-slate-700 mb-2">Port</label>
                    <input
                      type="text"
                      value={dbConfig.port}
                      onChange={(e) => setDbConfig({...dbConfig, port: e.target.value})}
                      className="w-full px-4 py-2 border border-slate-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                      placeholder="5432"
                    />
                  </div>
                  
                  <div>
                    <label className="block text-sm font-medium text-slate-700 mb-2">Database</label>
                    <input
                      type="text"
                      value={dbConfig.database}
                      onChange={(e) => setDbConfig({...dbConfig, database: e.target.value})}
                      className="w-full px-4 py-2 border border-slate-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                      placeholder="data_source"
                    />
                  </div>
                  
                  <div>
                    <label className="block text-sm font-medium text-slate-700 mb-2">Schema</label>
                    <input
                      type="text"
                      value={dbConfig.schema}
                      onChange={(e) => setDbConfig({...dbConfig, schema: e.target.value})}
                      className="w-full px-4 py-2 border border-slate-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                      placeholder="public"
                    />
                  </div>
                  
                  <div>
                    <label className="block text-sm font-medium text-slate-700 mb-2">User</label>
                    <input
                      type="text"
                      value={dbConfig.user}
                      onChange={(e) => setDbConfig({...dbConfig, user: e.target.value})}
                      className="w-full px-4 py-2 border border-slate-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                      placeholder="postgres"
                    />
                  </div>
                  
                  <div>
                    <label className="block text-sm font-medium text-slate-700 mb-2">Password</label>
                    <input
                      type="password"
                      value={dbConfig.password}
                      onChange={(e) => setDbConfig({...dbConfig, password: e.target.value})}
                      className="w-full px-4 py-2 border border-slate-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                      placeholder="••••••••"
                    />
                  </div>
                  
                  <div className="col-span-2">
                    <label className="block text-sm font-medium text-slate-700 mb-2">Table Name</label>
                    <input
                      type="text"
                      value={dbConfig.tableName}
                      onChange={(e) => setDbConfig({...dbConfig, tableName: e.target.value})}
                      className="w-full px-4 py-2 border border-slate-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                      placeholder="orders"
                    />
                  </div>
                </div>
              </div>
            )}

            {/* Sensor Configuration Tab */}
            {activeTab === 'sensor' && (
              <div className="space-y-6">
                <h3 className="text-lg font-semibold text-slate-900 mb-4">Sensor & Rule Configuration</h3>
                
                <div className="grid grid-cols-3 gap-4">
                  <div>
                    <label className="block text-sm font-medium text-slate-700 mb-2">Sensor Category</label>
                    <select
                      value={sensorConfig.sensorCategory}
                      onChange={(e) => {
                        setSensorConfig({
                          ...sensorConfig,
                          sensorCategory: e.target.value,
                          sensorType: '',
                          ruleName: ''
                        });
                      }}
                      className="w-full px-4 py-2 border border-slate-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                    >
                      <option value="">Select category...</option>
                      <option value="table">Table Level</option>
                      <option value="column">Column Level</option>
                    </select>
                  </div>
                  
                  <div>
                    <label className="block text-sm font-medium text-slate-700 mb-2">Sensor Type</label>
                    <select
                      value={sensorConfig.sensorType}
                      onChange={(e) => {
                        setSensorConfig({
                          ...sensorConfig,
                          sensorType: e.target.value,
                          ruleName: ''
                        });
                      }}
                      disabled={!sensorConfig.sensorCategory}
                      className="w-full px-4 py-2 border border-slate-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500 disabled:bg-slate-100"
                    >
                      <option value="">Select sensor type...</option>
                      {sensorConfig.sensorCategory && sensorCategories[sensorConfig.sensorCategory]?.sensors.map(sensor => (
                        <option key={sensor.value} value={sensor.value}>
                          {sensor.label}
                        </option>
                      ))}
                    </select>
                  </div>
                  
                  <div>
                    <label className="block text-sm font-medium text-slate-700 mb-2">Rule Name</label>
                    <select
                      value={sensorConfig.ruleName}
                      onChange={(e) => setSensorConfig({...sensorConfig, ruleName: e.target.value})}
                      disabled={!sensorConfig.sensorType}
                      className="w-full px-4 py-2 border border-slate-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500 disabled:bg-slate-100"
                    >
                      <option value="">Select rule...</option>
                      {sensorConfig.sensorType && sensorCategories[sensorConfig.sensorCategory]?.sensors
                        .find(s => s.value === sensorConfig.sensorType)?.rules.map(rule => (
                        <option key={rule} value={rule}>{rule}</option>
                      ))}
                    </select>
                  </div>
                </div>

                {sensorConfig.sensorCategory && (
                  <div className="bg-blue-50 border border-blue-200 rounded-lg p-4">
                    <div className="flex items-start space-x-3">
                      <AlertCircle className="w-5 h-5 text-blue-600 mt-0.5" />
                      <div>
                        <h4 className="font-medium text-blue-900">
                          Sensor Category: {sensorCategories[sensorConfig.sensorCategory]?.label}
                        </h4>
                        <p className="text-sm text-blue-700 mt-1">
                          {sensorConfig.sensorCategory === 'column' 
                            ? 'This sensor operates on individual columns. You will need to specify a column name in the Context tab.'
                            : 'This sensor operates on the entire table and analyzes table-level metrics.'}
                        </p>
                      </div>
                    </div>
                  </div>
                )}

                {sensorConfig.ruleName && (
                  <div className="bg-green-50 border border-green-200 rounded-lg p-4">
                    <div className="flex items-start space-x-3">
                      <CheckCircle className="w-5 h-5 text-green-600 mt-0.5" />
                      <div>
                        <h4 className="font-medium text-green-900">Rule Selected: {sensorConfig.ruleName}</h4>
                        <p className="text-sm text-green-700 mt-1">
                          {ruleCategories.average?.includes(sensorConfig.ruleName) && 'This rule calculates average values for numeric columns.'}
                          {ruleCategories.change?.includes(sensorConfig.ruleName) && 'This rule monitors changes and anomalies over time.'}
                          {ruleCategories.comparison?.includes(sensorConfig.ruleName) && 'This rule compares values against expected ranges or thresholds.'}
                        </p>
                      </div>
                    </div>
                  </div>
                )}
              </div>
            )}

            {/* Context Parameters Tab */}
            {activeTab === 'context' && (
              <div className="space-y-6">
                <h3 className="text-lg font-semibold text-slate-900 mb-4">Context Configuration</h3>
                
                {/* Target Table */}
                <div className="bg-slate-50 rounded-lg p-4 space-y-4">
                  <h4 className="font-medium text-slate-900">Target Table</h4>
                  <div className="grid grid-cols-2 gap-4">
                    <div>
                      <label className="block text-sm font-medium text-slate-700 mb-2">Schema Name</label>
                      <input
                        type="text"
                        value={context.targetTable.schemaName}
                        onChange={(e) => setContext({
                          ...context,
                          targetTable: {...context.targetTable, schemaName: e.target.value}
                        })}
                        className="w-full px-4 py-2 border border-slate-300 rounded-lg"
                        placeholder="bronze"
                      />
                    </div>
                    <div>
                      <label className="block text-sm font-medium text-slate-700 mb-2">Table Name</label>
                      <input
                        type="text"
                        value={context.targetTable.tableName}
                        onChange={(e) => setContext({
                          ...context,
                          targetTable: {...context.targetTable, tableName: e.target.value}
                        })}
                        className="w-full px-4 py-2 border border-slate-300 rounded-lg"
                        placeholder="data_source_orders"
                      />
                    </div>
                  </div>
                </div>

                {/* Table Configuration */}
                <div className="bg-slate-50 rounded-lg p-4 space-y-4">
                  <h4 className="font-medium text-slate-900">Table Configuration</h4>
                  <div className="grid grid-cols-2 gap-4">
                    <div>
                      <label className="block text-sm font-medium text-slate-700 mb-2">Filter (WHERE clause)</label>
                      <input
                        type="text"
                        value={context.table.filter}
                        onChange={(e) => setContext({
                          ...context,
                          table: {...context.table, filter: e.target.value}
                        })}
                        className="w-full px-4 py-2 border border-slate-300 rounded-lg"
                        placeholder="is_deleted = FALSE"
                      />
                    </div>
                    <div>
                      <label className="block text-sm font-medium text-slate-700 mb-2">Event Timestamp Column</label>
                      <input
                        type="text"
                        value={context.table.eventTimestampColumn}
                        onChange={(e) => setContext({
                          ...context,
                          table: {...context.table, eventTimestampColumn: e.target.value}
                        })}
                        className="w-full px-4 py-2 border border-slate-300 rounded-lg"
                        placeholder="created_at"
                      />
                    </div>
                  </div>
                </div>

                {/* Column Name (for column sensors) */}
                {sensorConfig.sensorCategory === 'column' && (
                  <div className="bg-slate-50 rounded-lg p-4">
                    <label className="block text-sm font-medium text-slate-700 mb-2">Column Name</label>
                    <input
                      type="text"
                      value={context.columnName}
                      onChange={(e) => setContext({...context, columnName: e.target.value})}
                      className="w-full px-4 py-2 border border-slate-300 rounded-lg"
                      placeholder="gender"
                    />
                  </div>
                )}

                {/* Error Sampling */}
                <div className="bg-slate-50 rounded-lg p-4 space-y-4">
                  <h4 className="font-medium text-slate-900">Error Sampling</h4>
                  <div className="grid grid-cols-3 gap-4">
                    <div>
                      <label className="block text-sm font-medium text-slate-700 mb-2">Samples Limit</label>
                      <input
                        type="number"
                        value={context.errorSampling.samplesLimit}
                        onChange={(e) => setContext({
                          ...context,
                          errorSampling: {...context.errorSampling, samplesLimit: e.target.value}
                        })}
                        className="w-full px-4 py-2 border border-slate-300 rounded-lg"
                      />
                    </div>
                    <div>
                      <label className="block text-sm font-medium text-slate-700 mb-2">Total Samples Limit</label>
                      <input
                        type="number"
                        value={context.errorSampling.totalSamplesLimit}
                        onChange={(e) => setContext({
                          ...context,
                          errorSampling: {...context.errorSampling, totalSamplesLimit: e.target.value}
                        })}
                        className="w-full px-4 py-2 border border-slate-300 rounded-lg"
                      />
                    </div>
                    <div>
                      <label className="block text-sm font-medium text-slate-700 mb-2">ID Columns</label>
                      <div className="flex space-x-2">
                        <input
                          type="text"
                          value={idColumnInput}
                          onChange={(e) => setIdColumnInput(e.target.value)}
                          onKeyPress={(e) => {
                            if (e.key === 'Enter') {
                              addToArray('idColumns', idColumnInput, (fn) => 
                                setContext({...context, errorSampling: fn(context.errorSampling)})
                              );
                              setIdColumnInput('');
                            }
                          }}
                          className="flex-1 px-4 py-2 border border-slate-300 rounded-lg"
                          placeholder="order_id"
                        />
                        <button
                          onClick={() => {
                            addToArray('idColumns', idColumnInput, (fn) => 
                              setContext({...context, errorSampling: fn(context.errorSampling)})
                            );
                            setIdColumnInput('');
                          }}
                          className="p-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700"
                        >
                          <Plus className="w-4 h-4" />
                        </button>
                      </div>
                      <div className="flex flex-wrap gap-2 mt-2">
                        {context.errorSampling.idColumns.map((col, idx) => (
                          <span key={idx} className="bg-blue-100 text-blue-800 px-3 py-1 rounded-full text-sm flex items-center space-x-1">
                            <span>{col}</span>
                            <button
                              onClick={() => setContext({
                                ...context,
                                errorSampling: {
                                  ...context.errorSampling,
                                  idColumns: context.errorSampling.idColumns.filter((_, i) => i !== idx)
                                }
                              })}
                            >
                              <Trash2 className="w-3 h-3" />
                            </button>
                          </span>
                        ))}
                      </div>
                    </div>
                  </div>
                </div>

                {/* Parameters */}
                <div className="bg-slate-50 rounded-lg p-4 space-y-4">
                  <h4 className="font-medium text-slate-900">Parameters</h4>
                  
                  <div className="grid grid-cols-2 gap-4">
                    <div>
                      <label className="block text-sm font-medium text-slate-700 mb-2">Filter Condition</label>
                      <input
                        type="text"
                        value={context.parameters.filter}
                        onChange={(e) => setContext({
                          ...context,
                          parameters: {...context.parameters, filter: e.target.value}
                        })}
                        className="w-full px-4 py-2 border border-slate-300 rounded-lg"
                        placeholder="num_of_item >= 2"
                      />
                    </div>

                    <div>
                      <label className="block text-sm font-medium text-slate-700 mb-2">Expected Value</label>
                      <input
                        type="text"
                        value={context.parameters.expectedValue}
                        onChange={(e) => setContext({
                          ...context,
                          parameters: {...context.parameters, expectedValue: e.target.value}
                        })}
                        className="w-full px-4 py-2 border border-slate-300 rounded-lg"
                        placeholder="100"
                      />
                    </div>

                    {ruleCategories.comparison?.includes(sensorConfig.ruleName) && (
                      <>
                        <div>
                          <label className="block text-sm font-medium text-slate-700 mb-2">Min Value</label>
                          <input
                            type="text"
                            value={context.parameters.minValue}
                            onChange={(e) => setContext({
                              ...context,
                              parameters: {...context.parameters, minValue: e.target.value}
                            })}
                            className="w-full px-4 py-2 border border-slate-300 rounded-lg"
                            placeholder="0"
                          />
                        </div>

                        <div>
                          <label className="block text-sm font-medium text-slate-700 mb-2">Max Value</label>
                          <input
                            type="text"
                            value={context.parameters.maxValue}
                            onChange={(e) => setContext({
                              ...context,
                              parameters: {...context.parameters, maxValue: e.target.value}
                            })}
                            className="w-full px-4 py-2 border border-slate-300 rounded-lg"
                            placeholder="100"
                          />
                        </div>
                      </>
                    )}
                  </div>

                  <div>
                    <label className="block text-sm font-medium text-slate-700 mb-2">SQL Expression</label>
                    <textarea
                      value={context.parameters.sqlExpression}
                      onChange={(e) => setContext({
                        ...context,
                        parameters: {...context.parameters, sqlExpression: e.target.value}
                      })}
                      className="w-full px-4 py-2 border border-slate-300 rounded-lg"
                      rows="3"
                      placeholder="100.0 * SUM(CASE WHEN email LIKE '%@%' THEN 1 ELSE 0 END) / COUNT(*)"
                    />
                  </div>

                  <div>
                    <label className="block text-sm font-medium text-slate-700 mb-2">SQL Condition</label>
                    <input
                      type="text"
                      value={context.parameters.sqlCondition}
                      onChange={(e) => setContext({
                        ...context,
                        parameters: {...context.parameters, sqlCondition: e.target.value}
                      })}
                      className="w-full px-4 py-2 border border-slate-300 rounded-lg"
                      placeholder="email LIKE '%@%'"
                    />
                  </div>
                </div>
              </div>
            )}

            {/* Results Tab */}
            {activeTab === 'results' && (
              <div className="space-y-6">
                {!results ? (
                  <div className="text-center py-12">
                    <Eye className="w-16 h-16 text-slate-300 mx-auto mb-4" />
                    <h3 className="text-lg font-medium text-slate-900 mb-2">No Results Yet</h3>
                    <p className="text-slate-500">Configure your quality check and click "Run Check" to see results</p>
                  </div>
                ) : (
                  <div className="space-y-6">
                    <div className="grid grid-cols-4 gap-4">
                      <div className="bg-blue-50 border border-blue-200 rounded-lg p-4">
                        <div className="text-sm font-medium text-blue-600 mb-1">Total Records</div>
                        <div className="text-2xl font-bold text-blue-900">{results.totalRecords}</div>
                      </div>
                      <div className="bg-green-50 border border-green-200 rounded-lg p-4">
                        <div className="text-sm font-medium text-green-600 mb-1">Passed</div>
                        <div className="text-2xl font-bold text-green-900">{results.passedRecords}</div>
                      </div>
                      <div className="bg-red-50 border border-red-200 rounded-lg p-4">
                        <div className="text-sm font-medium text-red-600 mb-1">Failed</div>
                        <div className="text-2xl font-bold text-red-900">{results.failedRecords}</div>
                      </div>
                      <div className="bg-purple-50 border border-purple-200 rounded-lg p-4">
                        <div className="text-sm font-medium text-purple-600 mb-1">Quality Score</div>
                        <div className="text-2xl font-bold text-purple-900">{results.qualityScore}%</div>
                      </div>
                    </div>

                    <div className="bg-white border border-slate-200 rounded-lg overflow-hidden">
                      <table className="w-full">
                        <thead className="bg-slate-50">
                          <tr>
                            <th className="px-6 py-3 text-left text-sm font-medium text-slate-700">Rule</th>
                            <th className="px-6 py-3 text-left text-sm font-medium text-slate-700">Passed</th>
                            <th className="px-6 py-3 text-left text-sm font-medium text-slate-700">Failed</th>
                            <th className="px-6 py-3 text-left text-sm font-medium text-slate-700">Success Rate</th>
                          </tr>
                        </thead>
                        <tbody>
                          {results.details.map((detail, idx) => (
                            <tr key={idx} className="border-t border-slate-200">
                              <td className="px-6 py-4 text-sm text-slate-900">{detail.rule}</td>
                              <td className="px-6 py-4 text-sm text-green-600">{detail.passed}</td>
                              <td className="px-6 py-4 text-sm text-red-600">{detail.failed}</td>
                              <td className="px-6 py-4 text-sm text-slate-900">{detail.percentage}%</td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  </div>
                )}
              </div>
            )}

            {/* Data Visualization Tab */}
            {activeTab === 'data-viz' && renderDataVisualization()}

            {/* Quality Visualization Tab */}
            {activeTab === 'quality-viz' && renderQualityVisualization()}
          </div>
        </div>
      </div>
    </div>
  );
}
