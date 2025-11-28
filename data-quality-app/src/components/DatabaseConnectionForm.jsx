// DatabaseConnectionForm.jsx
import React, { useState } from 'react';
import { Server, Database, User, Lock, Plus, Trash2, CheckCircle, XCircle, Loader2, ChevronRight } from 'lucide-react';

export default function DatabaseConnectionForm({
  dbConfig,
  setDbConfig,
  connectionStatus,
  setConnectionStatus,
  proceedToSchemaSelection
}) {
  const [isConnecting, setIsConnecting] = useState(false);
  const [newJdbcKey, setNewJdbcKey] = useState('');
  const [newJdbcValue, setNewJdbcValue] = useState('');

  const updateConfig = (field, value) => {
    setDbConfig(prev => ({ ...prev, [field]: value }));
    // Reset connection status when config changes
    if (connectionStatus) {
      setConnectionStatus(null);
    }
  };

  const addJdbcProperty = () => {
    if (newJdbcKey && newJdbcValue) {
      setDbConfig(prev => ({
        ...prev,
        jdbcProperties: [...(prev.jdbcProperties || []), { key: newJdbcKey, value: newJdbcValue }]
      }));
      setNewJdbcKey('');
      setNewJdbcValue('');
    }
  };

  const removeJdbcProperty = (index) => {
    setDbConfig(prev => ({
      ...prev,
      jdbcProperties: prev.jdbcProperties.filter((_, i) => i !== index)
    }));
  };

  const testConnection = async () => {
    setIsConnecting(true);
    setConnectionStatus(null);

    try {
      // Validate required fields
      if (!dbConfig.connectionName || !dbConfig.host || !dbConfig.port || !dbConfig.username || !dbConfig.database) {
        throw new Error('Please fill in all required fields');
      }

      // Convert jdbcProperties array to object
      const jdbcPropertiesObj = dbConfig.jdbcProperties && dbConfig.jdbcProperties.length > 0
        ? dbConfig.jdbcProperties.reduce((acc, prop) => {
            acc[prop.key] = prop.value;
            return acc;
          }, {})
        : {};

      const payload = {
        connectionName: dbConfig.connectionName,
        host: dbConfig.host,
        port: dbConfig.port,
        username: dbConfig.username,
        password: dbConfig.password,
        database: dbConfig.database,
        jdbcProperties: jdbcPropertiesObj,
        savedAt: new Date().toISOString()
      };

      console.log('Testing connection with payload:', payload);

      const response = await fetch('http://localhost:8000/create_connection', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(payload)
      });

      const data = await response.json();

      if (!response.ok) {
        throw new Error(data.detail || 'Connection failed');
      }

      console.log('Connection successful:', data);
      setConnectionStatus('success');

    } catch (error) {
      console.error('Connection error:', error);
      setConnectionStatus('error');
      alert(`Connection failed: ${error.message}`);
    } finally {
      setIsConnecting(false);
    }
  };

  return (
    <div className="bg-white rounded-lg shadow-sm border border-slate-200 p-8">
      <div className="space-y-6">
        {/* Connection Name */}
        <div>
          <label className="block text-sm font-medium text-slate-700 mb-2">
            Connection Name *
          </label>
          <div className="relative">
            <Database className="absolute left-3 top-1/2 transform -translate-y-1/2 w-5 h-5 text-slate-400" />
            <input
              type="text"
              value={dbConfig.connectionName}
              onChange={(e) => updateConfig('connectionName', e.target.value)}
              placeholder="e.g., production-db"
              className="w-full pl-10 pr-4 py-2.5 border border-slate-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
              required
            />
          </div>
        </div>

        {/* Host and Port */}
        <div className="grid grid-cols-3 gap-4">
          <div className="col-span-2">
            <label className="block text-sm font-medium text-slate-700 mb-2">
              Host *
            </label>
            <div className="relative">
              <Server className="absolute left-3 top-1/2 transform -translate-y-1/2 w-5 h-5 text-slate-400" />
              <input
                type="text"
                value={dbConfig.host}
                onChange={(e) => updateConfig('host', e.target.value)}
                placeholder="localhost or IP address"
                className="w-full pl-10 pr-4 py-2.5 border border-slate-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                required
              />
            </div>
          </div>
          <div>
            <label className="block text-sm font-medium text-slate-700 mb-2">
              Port *
            </label>
            <input
              type="text"
              value={dbConfig.port}
              onChange={(e) => updateConfig('port', e.target.value)}
              placeholder="5432"
              className="w-full px-4 py-2.5 border border-slate-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
              required
            />
          </div>
        </div>

        {/* Database Name */}
        <div>
          <label className="block text-sm font-medium text-slate-700 mb-2">
            Database Name *
          </label>
          <div className="relative">
            <Database className="absolute left-3 top-1/2 transform -translate-y-1/2 w-5 h-5 text-slate-400" />
            <input
              type="text"
              value={dbConfig.database}
              onChange={(e) => updateConfig('database', e.target.value)}
              placeholder="postgres"
              className="w-full pl-10 pr-4 py-2.5 border border-slate-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
              required
            />
          </div>
        </div>

        {/* Username and Password */}
        <div className="grid grid-cols-2 gap-4">
          <div>
            <label className="block text-sm font-medium text-slate-700 mb-2">
              Username *
            </label>
            <div className="relative">
              <User className="absolute left-3 top-1/2 transform -translate-y-1/2 w-5 h-5 text-slate-400" />
              <input
                type="text"
                value={dbConfig.username}
                onChange={(e) => updateConfig('username', e.target.value)}
                placeholder="postgres"
                className="w-full pl-10 pr-4 py-2.5 border border-slate-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                required
              />
            </div>
          </div>
          <div>
            <label className="block text-sm font-medium text-slate-700 mb-2">
              Password *
            </label>
            <div className="relative">
              <Lock className="absolute left-3 top-1/2 transform -translate-y-1/2 w-5 h-5 text-slate-400" />
              <input
                type="password"
                value={dbConfig.password}
                onChange={(e) => updateConfig('password', e.target.value)}
                placeholder="••••••••"
                className="w-full pl-10 pr-4 py-2.5 border border-slate-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                required
              />
            </div>
          </div>
        </div>

        {/* JDBC Properties */}
        <div>
          <label className="block text-sm font-medium text-slate-700 mb-2">
            JDBC Properties (Optional)
          </label>
          
          {/* Existing Properties */}
          {dbConfig.jdbcProperties && dbConfig.jdbcProperties.length > 0 && (
            <div className="space-y-2 mb-3">
              {dbConfig.jdbcProperties.map((prop, index) => (
                <div key={index} className="flex items-center space-x-2 bg-slate-50 p-3 rounded-lg">
                  <span className="flex-1 text-sm font-mono text-slate-700">
                    {prop.key} = {prop.value}
                  </span>
                  <button
                    onClick={() => removeJdbcProperty(index)}
                    className="text-red-600 hover:text-red-700"
                  >
                    <Trash2 className="w-4 h-4" />
                  </button>
                </div>
              ))}
            </div>
          )}

          {/* Add New Property */}
          <div className="flex space-x-2">
            <input
              type="text"
              value={newJdbcKey}
              onChange={(e) => setNewJdbcKey(e.target.value)}
              placeholder="Key (e.g., ssl)"
              className="flex-1 px-4 py-2 border border-slate-300 rounded-lg focus:ring-2 focus:ring-blue-500"
            />
            <input
              type="text"
              value={newJdbcValue}
              onChange={(e) => setNewJdbcValue(e.target.value)}
              placeholder="Value (e.g., true)"
              className="flex-1 px-4 py-2 border border-slate-300 rounded-lg focus:ring-2 focus:ring-blue-500"
            />
            <button
              onClick={addJdbcProperty}
              disabled={!newJdbcKey || !newJdbcValue}
              className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:bg-slate-300"
            >
              <Plus className="w-5 h-5" />
            </button>
          </div>
        </div>

        {/* Connection Status */}
        {connectionStatus && (
          <div className={`p-4 rounded-lg flex items-center space-x-3 ${
            connectionStatus === 'success' 
              ? 'bg-green-50 border border-green-200' 
              : 'bg-red-50 border border-red-200'
          }`}>
            {connectionStatus === 'success' ? (
              <>
                <CheckCircle className="w-5 h-5 text-green-600" />
                <span className="text-green-800 font-medium">Connection successful!</span>
              </>
            ) : (
              <>
                <XCircle className="w-5 h-5 text-red-600" />
                <span className="text-red-800 font-medium">Connection failed. Please check your credentials.</span>
              </>
            )}
          </div>
        )}

        {/* Action Buttons */}
        <div className="flex justify-end space-x-3 pt-4">
          <button
            onClick={testConnection}
            disabled={isConnecting}
            className="flex items-center space-x-2 bg-blue-600 text-white px-6 py-2.5 rounded-lg hover:bg-blue-700 disabled:bg-slate-400 disabled:cursor-not-allowed transition-colors"
          >
            {isConnecting ? (
              <>
                <Loader2 className="w-4 h-4 animate-spin" />
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
            <button
              onClick={proceedToSchemaSelection}
              className="flex items-center space-x-2 bg-green-600 text-white px-6 py-2.5 rounded-lg hover:bg-green-700 transition-colors"
            >
              <span>Proceed to Schema Selection</span>
              <ChevronRight className="w-4 h-4" />
            </button>
          )}
        </div>
      </div>
    </div>
  );
}