// App.jsx - Main Component
import React, { useState } from 'react';
import { Database, ChevronRight, Check } from 'lucide-react';
import DatabaseConnectionForm from './DatabaseConnectionForm';
import SchemaTableSelector from './SchemaTableSelector';
import DataQualityDashboard from './DataQualityDashboard';

export default function DataQualityApp() {
  const [step, setStep] = useState(1);
  const [connectionStatus, setConnectionStatus] = useState(null);
  const [showDashboard, setShowDashboard] = useState(false);
  
  const [dbConfig, setDbConfig] = useState({
    connection_name: '',
    host: '',
    port: '5432',
    username: '',
    password: '',
    database: '',
    jdbc_properties: []
  });

  const [schemas, setSchemas] = useState([]);
  const [selectedTables, setSelectedTables] = useState([]);
  const [isLoadingSchemas, setIsLoadingSchemas] = useState(false);
  const [ingestedTables, setIngestedTables] = useState([]);

  const fetchSchemas = async () => {
    setIsLoadingSchemas(true);
    setSchemas([]);
    
    try {
      const response = await fetch(`http://localhost:8000/postgres/schemas/${dbConfig.connection_name}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' }
      });

      if (!response.ok) {
        const errorData = await response.json();
        throw new Error(errorData.detail || 'Failed to fetch schemas');
      }

      const data = await response.json();
      
      const formattedSchemas = data.schemas.map(schema => ({
        name: schema.schema_name,
        expanded: false,
        tables: schema.tables || [],
        table_count: schema.table_count || 0
      }));

      setSchemas(formattedSchemas);
    } catch (error) {
      alert(`Failed to fetch schemas: ${error.message}`);
      setSchemas([]);
    } finally {
      setIsLoadingSchemas(false);
    }
  };

  const proceedToSchemaSelection = async () => {
    if (connectionStatus === 'success') {
      setStep(2);
      await fetchSchemas();
    }
  };

  const handleIngestionComplete = (tablesData) => {
    // tablesData should be array of { schema, table, primary_keys }
    setIngestedTables(tablesData);
    // Navigate to dashboard after successful ingestion
    setShowDashboard(true);
  };

  const handleLogout = () => {
    // Reset to initial state
    setShowDashboard(false);
    setStep(1);
    setConnectionStatus(null);
    setDbConfig({
      connection_name: '',
      host: '',
      port: '5432',
      username: '',
      password: '',
      database: '',
      jdbc_properties: []
    });
    setSchemas([]);
    setSelectedTables([]);
    setIngestedTables([]);
  };

  // Show dashboard if ingestion is complete
  if (showDashboard) {
    return (
      <DataQualityDashboard 
        connectionName={dbConfig.connection_name}
        ingestedTables={ingestedTables}
        onLogout={handleLogout}
      />
    );
  }

  return (
    <div className="min-h-screen bg-gradient-to-br from-slate-50 to-slate-100">
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
        {step === 1 && (
          <DatabaseConnectionForm
            dbConfig={dbConfig}
            setDbConfig={setDbConfig}
            connectionStatus={connectionStatus}
            setConnectionStatus={setConnectionStatus}
            proceedToSchemaSelection={proceedToSchemaSelection}
          />
        )}

        {step === 2 && (
          <SchemaTableSelector
            schemas={schemas}
            setSchemas={setSchemas}
            selectedTables={selectedTables}
            setSelectedTables={setSelectedTables}
            setStep={setStep}
            isLoadingSchemas={isLoadingSchemas}
            dbConfig={dbConfig}
            onIngestionComplete={handleIngestionComplete}
          />
        )}
      </div>
    </div>
  );
}