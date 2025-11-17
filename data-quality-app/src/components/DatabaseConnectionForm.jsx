// DatabaseConnectionForm.jsx
import React, { useState } from 'react';
import { Plus, Trash2, Database, RefreshCw, Check, ChevronRight } from 'lucide-react';

export default function DatabaseConnectionForm({
  dbConfig,
  setDbConfig,
  connectionStatus,
  setConnectionStatus,
  saveConfigToFile,
  proceedToSchemaSelection
}) {
  const [isTestingConnection, setIsTestingConnection] = useState(false);
  const [newProperty, setNewProperty] = useState({ key: '', value: '' });

  // Predefined JDBC properties
  const jdbcPropertyOptions = [
    'ssl',
    'sslmode',
    'connectTimeout',
    'socketTimeout',
    'applicationName',
    'loginTimeout',
    'prepareThreshold',
    'binaryTransfer',
    'tcpKeepAlive',
    'defaultRowFetchSize'
  ];

  const addJdbcProperty = () => {
    if (newProperty.key && newProperty.value) {
      setDbConfig({
        ...dbConfig,
        jdbcProperties: [...dbConfig.jdbcProperties, { ...newProperty, id: Date.now() }]
      });
      setNewProperty({ key: '', value: '' });
    }
  };

  const removeJdbcProperty = (id) => {
    setDbConfig({
      ...dbConfig,
      jdbcProperties: dbConfig.jdbcProperties.filter(prop => prop.id !== id)
    });
  };

  const testConnection = () => {
    setIsTestingConnection(true);
    // Simulate API call
    setTimeout(() => {
      setIsTestingConnection(false);
      setConnectionStatus('success');
      saveConfigToFile();
    }, 2000);
  };

  return (
    <div className="space-y-6">
      <div className="bg-white rounded-lg shadow-sm border border-slate-200 p-6">
        <h3 className="text-lg font-semibold text-slate-900 mb-6">Database Connection</h3>
        
        <div className="space-y-4">
          {/* Connection Name */}
          <div>
            <label className="block text-sm font-medium text-slate-700 mb-2">
              Connection Name <span className="text-red-500">*</span>
            </label>
            <input
              type="text"
              value={dbConfig.connectionName}
              onChange={(e) => setDbConfig({...dbConfig, connectionName: e.target.value})}
              className="w-full px-4 py-2 border border-slate-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
              placeholder="Please enter the connection name."
            />
          </div>

          {/* Host and Port */}
          <div className="grid grid-cols-3 gap-4">
            <div className="col-span-2">
              <label className="block text-sm font-medium text-slate-700 mb-2">
                Host <span className="text-red-500">*</span>
              </label>
              <input
                type="text"
                value={dbConfig.host}
                onChange={(e) => setDbConfig({...dbConfig, host: e.target.value})}
                className="w-full px-4 py-2 border border-slate-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                placeholder="data_source"
              />
            </div>
            <div>
              <label className="block text-sm font-medium text-slate-700 mb-2">
                Port <span className="text-red-500">*</span>
              </label>
              <input
                type="text"
                value={dbConfig.port}
                onChange={(e) => setDbConfig({...dbConfig, port: e.target.value})}
                className="w-full px-4 py-2 border border-slate-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                placeholder="5432"
              />
            </div>
          </div>

          {/* Username and Password */}
          <div className="grid grid-cols-2 gap-4">
            <div>
              <label className="block text-sm font-medium text-slate-700 mb-2">
                Username <span className="text-red-500">*</span>
              </label>
              <input
                type="text"
                value={dbConfig.username}
                onChange={(e) => setDbConfig({...dbConfig, username: e.target.value})}
                className="w-full px-4 py-2 border border-slate-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                placeholder="postgres"
              />
            </div>
            <div>
              <label className="block text-sm font-medium text-slate-700 mb-2">
                Password <span className="text-red-500">*</span>
              </label>
              <input
                type="password"
                value={dbConfig.password}
                onChange={(e) => setDbConfig({...dbConfig, password: e.target.value})}
                className="w-full px-4 py-2 border border-slate-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                placeholder="••••••••"
              />
            </div>
          </div>

          {/* Database */}
          <div>
            <label className="block text-sm font-medium text-slate-700 mb-2">
              Database <span className="text-red-500">*</span>
            </label>
            <input
              type="text"
              value={dbConfig.database}
              onChange={(e) => setDbConfig({...dbConfig, database: e.target.value})}
              className="w-full px-4 py-2 border border-slate-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
              placeholder="Please enter the database name"
            />
          </div>

          {/* JDBC Connection Properties */}
          <div className="border-t border-slate-200 pt-4 mt-6">
            <div className="flex items-center justify-between mb-4">
              <h4 className="font-medium text-slate-900">JDBC Connection Properties</h4>
            </div>

            {/* Add New Property */}
            <div className="bg-slate-50 rounded-lg p-4 mb-4">
              <div className="grid grid-cols-12 gap-3">
                <div className="col-span-5">
                  <label className="block text-xs font-medium text-slate-600 mb-1">Property</label>
                  <select
                    value={newProperty.key}
                    onChange={(e) => setNewProperty({...newProperty, key: e.target.value})}
                    className="w-full px-3 py-2 text-sm border border-slate-300 rounded-lg focus:ring-2 focus:ring-blue-500"
                  >
                    <option value="">Select property...</option>
                    {jdbcPropertyOptions.map(prop => (
                      <option key={prop} value={prop}>{prop}</option>
                    ))}
                  </select>
                </div>
                <div className="col-span-6">
                  <label className="block text-xs font-medium text-slate-600 mb-1">Value</label>
                  <input
                    type="text"
                    value={newProperty.value}
                    onChange={(e) => setNewProperty({...newProperty, value: e.target.value})}
                    onKeyPress={(e) => e.key === 'Enter' && addJdbcProperty()}
                    className="w-full px-3 py-2 text-sm border border-slate-300 rounded-lg focus:ring-2 focus:ring-blue-500"
                    placeholder="Enter value..."
                  />
                </div>
                <div className="col-span-1 flex items-end">
                  <button
                    onClick={addJdbcProperty}
                    disabled={!newProperty.key || !newProperty.value}
                    className="w-full p-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:bg-slate-300 disabled:cursor-not-allowed"
                  >
                    <Plus className="w-4 h-4 mx-auto" />
                  </button>
                </div>
              </div>
            </div>

            {/* Properties List */}
            {dbConfig.jdbcProperties.length > 0 && (
              <div className="space-y-2">
                <div className="grid grid-cols-12 gap-3 px-3 pb-2 text-xs font-medium text-slate-600 border-b">
                  <div className="col-span-5">Property</div>
                  <div className="col-span-6">Value</div>
                  <div className="col-span-1">Action</div>
                </div>
                {dbConfig.jdbcProperties.map((prop) => (
                  <div key={prop.id} className="grid grid-cols-12 gap-3 items-center bg-white border border-slate-200 rounded-lg p-3">
                    <div className="col-span-5 font-medium text-sm text-slate-900">{prop.key}</div>
                    <div className="col-span-6 text-sm text-slate-600">{prop.value}</div>
                    <div className="col-span-1">
                      <button
                        onClick={() => removeJdbcProperty(prop.id)}
                        className="p-1 text-red-600 hover:bg-red-50 rounded"
                      >
                        <Trash2 className="w-4 h-4" />
                      </button>
                    </div>
                  </div>
                ))}
              </div>
            )}
          </div>
        </div>

        {/* Test Connection Button */}
        <div className="mt-6 flex items-center space-x-4">
          <button
            onClick={testConnection}
            disabled={!dbConfig.connectionName || !dbConfig.host || !dbConfig.username || !dbConfig.database || isTestingConnection}
            className="flex items-center space-x-2 bg-blue-600 text-white px-6 py-2.5 rounded-lg hover:bg-blue-700 disabled:bg-slate-300 disabled:cursor-not-allowed transition-colors"
          >
            {isTestingConnection ? (
              <>
                <RefreshCw className="w-4 h-4 animate-spin" />
                <span>Testing Connection...</span>
              </>
            ) : (
              <>
                <Database className="w-4 h-4" />
                <span>Test Connection</span>
              </>
            )}
          </button>

          {connectionStatus === 'success' && (
            <div className="flex items-center space-x-2 text-green-600">
              <Check className="w-5 h-5" />
              <span className="font-medium">Connection Successful!</span>
            </div>
          )}
        </div>
      </div>

      {/* Configuration Preview */}
      {connectionStatus === 'success' && (
        <div className="bg-white rounded-lg shadow-sm border border-slate-200 p-6">
          <div className="flex items-center justify-between mb-4">
            <h3 className="text-lg font-semibold text-slate-900">Configuration Preview</h3>
            <span className="text-xs text-slate-500">Saved to: /configs/db_connections.json</span>
          </div>
          <pre className="bg-slate-900 text-green-400 p-4 rounded-lg text-sm overflow-x-auto">
{JSON.stringify({
  connectionName: dbConfig.connectionName,
  host: dbConfig.host,
  port: dbConfig.port,
  username: dbConfig.username,
  database: dbConfig.database,
  jdbcProperties: dbConfig.jdbcProperties.reduce((acc, prop) => {
    acc[prop.key] = prop.value;
    return acc;
  }, {}),
  savedAt: new Date().toISOString()
}, null, 2)}
          </pre>

          <div className="mt-4 flex justify-end">
            <button
              onClick={proceedToSchemaSelection}
              className="flex items-center space-x-2 bg-green-600 text-white px-6 py-2.5 rounded-lg hover:bg-green-700 transition-colors"
            >
              <span>Proceed to Schema Selection</span>
              <ChevronRight className="w-4 h-4" />
            </button>
          </div>
        </div>
      )}
    </div>
  );
}