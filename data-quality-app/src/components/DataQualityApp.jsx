// App.jsx - Main Component
import React, { useState } from 'react';
import { Database, ChevronRight, Check } from 'lucide-react';
import DatabaseConnectionForm from './DatabaseConnectionForm';
import SchemaTableSelector from './SchemaTableSelector';

export default function DataQualityApp() {
  const [step, setStep] = useState(1); // 1: Connection, 2: Schema Selection
  const [connectionStatus, setConnectionStatus] = useState(null);
  
  const [dbConfig, setDbConfig] = useState({
    connectionName: '',
    host: '',
    port: '5432',
    username: '',
    password: '',
    database: '',
    jdbcProperties: []
  });

  const [schemas, setSchemas] = useState([
    {
      name: 'public',
      expanded: false,
      tables: ['users', 'orders', 'products', 'transactions']
    },
    {
      name: 'bronze',
      expanded: false,
      tables: ['raw_data', 'staging_orders', 'staging_users']
    },
    {
      name: 'silver',
      expanded: false,
      tables: ['cleaned_data', 'transformed_orders']
    }
  ]);

  const [selectedTables, setSelectedTables] = useState([]);

  const saveConfigToFile = () => {
    const config = {
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
    };
    
    console.log('Config saved to: /configs/db_connections.json', config);
    localStorage.setItem('db_connection_config', JSON.stringify(config));
    return config;
  };

  const proceedToSchemaSelection = () => {
    if (connectionStatus === 'success') {
      setStep(2);
    }
  };

  const submitSelectedTables = () => {
    const selectedData = selectedTables.map(tableId => {
      const [schema, table] = tableId.split('.');
      return { schema, table };
    });
    
    console.log('Selected tables submitted:', selectedData);
    alert(`Successfully added ${selectedTables.length} tables to Data Quality system!`);
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
                <p className="text-sm text-slate-500">
                  {step === 1 ? 'Configure Database Connection' : 'Select Schemas and Tables'}
                </p>
              </div>
            </div>
            
            {/* Step Indicator */}
            <div className="flex items-center space-x-2">
              <div className={`flex items-center space-x-2 px-4 py-2 rounded-lg ${
                step === 1 ? 'bg-blue-100 text-blue-700' : 'bg-green-100 text-green-700'
              }`}>
                <div className={`w-6 h-6 rounded-full flex items-center justify-center text-xs font-bold ${
                  step === 1 ? 'bg-blue-600 text-white' : 'bg-green-600 text-white'
                }`}>
                  {step === 1 ? '1' : <Check className="w-4 h-4" />}
                </div>
                <span className="text-sm font-medium">Connection</span>
              </div>
              <ChevronRight className="w-5 h-5 text-slate-400" />
              <div className={`flex items-center space-x-2 px-4 py-2 rounded-lg ${
                step === 2 ? 'bg-blue-100 text-blue-700' : 'bg-slate-100 text-slate-500'
              }`}>
                <div className={`w-6 h-6 rounded-full flex items-center justify-center text-xs font-bold ${
                  step === 2 ? 'bg-blue-600 text-white' : 'bg-slate-300 text-slate-600'
                }`}>
                  2
                </div>
                <span className="text-sm font-medium">Schema Selection</span>
              </div>
            </div>
          </div>
        </div>
      </div>

      <div className="max-w-7xl mx-auto px-6 py-8">
        {/* STEP 1: Database Connection */}
        {step === 1 && (
          <DatabaseConnectionForm
            dbConfig={dbConfig}
            setDbConfig={setDbConfig}
            connectionStatus={connectionStatus}
            setConnectionStatus={setConnectionStatus}
            saveConfigToFile={saveConfigToFile}
            proceedToSchemaSelection={proceedToSchemaSelection}
          />
        )}

        {/* STEP 2: Schema Selection */}
        {step === 2 && (
          <SchemaTableSelector
            schemas={schemas}
            setSchemas={setSchemas}
            selectedTables={selectedTables}
            setSelectedTables={setSelectedTables}
            setStep={setStep}
            submitSelectedTables={submitSelectedTables}
          />
        )}
      </div>
    </div>
  );
}