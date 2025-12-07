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

  const [schemas, setSchemas] = useState([]);
  const [selectedTables, setSelectedTables] = useState([]);
  const [isLoadingSchemas, setIsLoadingSchemas] = useState(false);

  const fetchSchemas = async () => {
    setIsLoadingSchemas(true);
    setSchemas([]);
    
    try {
      console.log('Fetching schemas for connection:', dbConfig.connectionName);
      
      // Convert jdbcProperties array to object
      const jdbcPropertiesObj = dbConfig.jdbcProperties && dbConfig.jdbcProperties.length > 0
        ? dbConfig.jdbcProperties.reduce((acc, prop) => {
            acc[prop.key] = prop.value;
            return acc;
          }, {})
        : {};
      
      // Call API to get schemas - simplified payload
      const response = await fetch('http://localhost:8000/postgres/schemas/' + dbConfig.connectionName, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        }
      });

      if (!response.ok) {
        const errorData = await response.json();
        throw new Error(errorData.detail || 'Failed to fetch schemas');
      }

      const data = await response.json();
      console.log('Schemas fetched:', data);
      
      // Transform data to match our schema structure
      const formattedSchemas = data.schemas.map(schema => ({
        name: schema.schema_name,
        expanded: false,
        tables: schema.tables || [],
        tableCount: schema.table_count || 0
      }));

      setSchemas(formattedSchemas);
      
      if (formattedSchemas.length === 0) {
        console.warn('No schemas found in database');
      } else {
        console.log(`Loaded ${formattedSchemas.length} schemas with tables`);
      }
      
    } catch (error) {
      console.error('Error fetching schemas:', error);
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

  const submitSelectedTables = async () => {
    if (selectedTables.length === 0) {
      alert('Please select at least one table');
      return;
    }

    const selectedData = selectedTables.map(tableId => {
      const [schema, table] = tableId.split('.');
      return { schema, table };
    });
    
    try {
      console.log('Submitting tables:', selectedData);
      
      // Call API to submit selected tables
      const response = await fetch('http://localhost:8000/submit_tables', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          connectionName: dbConfig.connectionName,
          tables: selectedData
        })
      });

      if (!response.ok) {
        const errorData = await response.json();
        throw new Error(errorData.detail || 'Failed to submit tables');
      }

      const result = await response.json();
      console.log('Tables submitted successfully:', result);
      
      alert(`Successfully added ${selectedTables.length} tables to Data Quality system!\n\nConfig saved to: ${result.configFile}`);
      
    } catch (error) {
      console.error('Error submitting tables:', error);
      alert(`Failed to submit tables: ${error.message}`);
    }
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
            isLoadingSchemas={isLoadingSchemas}
            dbConfig={dbConfig}
          />
        )}
      </div>
    </div>
  );
}