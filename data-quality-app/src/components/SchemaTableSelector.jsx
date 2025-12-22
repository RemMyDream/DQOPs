import React, { useState, useEffect } from 'react';
import { Trash2, Database, Check, ChevronDown, ChevronRight, RefreshCw, Loader2, Search, X, Eye, CheckSquare, AlertTriangle, Key } from 'lucide-react';

export default function SchemaTableSelector({
  schemas,
  setSchemas,
  selectedTables,
  setSelectedTables,
  setStep,
  isLoadingSchemas,
  dbConfig,
  onIngestionComplete  // THÊM prop này
}) {
  const [searchTerm, setSearchTerm] = useState('');
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [showPreviewModal, setShowPreviewModal] = useState(false);
  const [previewData, setPreviewData] = useState(null);
  const [loadingPreview, setLoadingPreview] = useState(false);
  const [showPrimaryKeyModal, setShowPrimaryKeyModal] = useState(false);
  const [primaryKeyData, setPrimaryKeyData] = useState(null);
  const [confirmedPrimaryKeys, setConfirmedPrimaryKeys] = useState({});
  const [loadingPrimaryKeys, setLoadingPrimaryKeys] = useState({});
  
  const [ingestionProgress, setIngestionProgress] = useState({});
  const [showProgressModal, setShowProgressModal] = useState(false);
  const [pollingIntervals, setPollingIntervals] = useState({});

  useEffect(() => {
    return () => {
      Object.values(pollingIntervals).forEach(interval => clearInterval(interval));
    };
  }, [pollingIntervals]);

  useEffect(() => {
    const checkNewlySelectedTables = async () => {
      for (const tableId of selectedTables) {
        if (!confirmedPrimaryKeys[tableId] && !loadingPrimaryKeys[tableId]) {
          const [schema, table] = tableId.split('.');
          await checkPrimaryKeys(schema, table, true);
        }
      }
    };
    
    checkNewlySelectedTables();
  }, [selectedTables]);

  const toggleSchema = (schemaName) => {
    setSchemas(schemas.map(s => 
      s.name === schemaName ? { ...s, expanded: !s.expanded } : s
    ));
  };

  const toggleTableSelection = (schemaName, tableName) => {
    const tableId = `${schemaName}.${tableName}`;
    if (selectedTables.includes(tableId)) {
      setSelectedTables(selectedTables.filter(id => id !== tableId));
      const newConfirmedPKs = { ...confirmedPrimaryKeys };
      delete newConfirmedPKs[tableId];
      setConfirmedPrimaryKeys(newConfirmedPKs);
    } else {
      setSelectedTables([...selectedTables, tableId]);
    }
  };

  const selectAllTablesInSchema = (schemaName) => {
    const schema = schemas.find(s => s.name === schemaName);
    if (!schema) return;

    const tablesToAdd = schema.tables
      .filter(table => !selectedTables.includes(`${schemaName}.${table}`))
      .map(table => `${schemaName}.${table}`);

    setSelectedTables([...selectedTables, ...tablesToAdd]);
  };

  const previewTable = async (schemaName, tableName) => {
    setLoadingPreview(true);
    setShowPreviewModal(true);
    setPreviewData(null);

    try {
      const payload = {
        connection_name: dbConfig.connection_name,
        schema_name: schemaName,
        table_name: tableName
      };
      
      const response = await fetch('http://localhost:8000/postgres/tables/preview', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
      });

      if (!response.ok) {
        const errorData = await response.json();
        throw new Error(errorData.detail || 'Failed to preview table');
      }

      const data = await response.json();
      
      if (data.rows?.length > 0) {
        const columns = Object.keys(data.rows[0]);
        const rowsAsArrays = data.rows.map(row => columns.map(col => row[col]));
        
        setPreviewData({
          schema: schemaName,
          table: tableName,
          columns,
          rows: rowsAsArrays,
          row_count: data.rows.length
        });
      } else {
        setPreviewData({
          schema: schemaName,
          table: tableName,
          columns: [],
          rows: [],
          row_count: 0
        });
      }
    } catch (error) {
      alert(`Failed to preview table: ${error.message}`);
      setShowPreviewModal(false);
    } finally {
      setLoadingPreview(false);
    }
  };

  const checkPrimaryKeys = async (schemaName, tableName, silent = false) => {
    const tableId = `${schemaName}.${tableName}`;
    setLoadingPrimaryKeys(prev => ({ ...prev, [tableId]: true }));
    
    try {
      const payload = {
        connection_name: dbConfig.connection_name,
        schema_name: schemaName,
        table_name: tableName
      };
      
      const response = await fetch('http://localhost:8000/postgres/tables/primary-keys', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
      });

      if (!response.ok) {
        const errorData = await response.json();
        throw new Error(errorData.detail || 'Failed to get primary keys');
      }

      const data = await response.json();
      
      if (data.has_primary_keys && data.primary_keys.length > 0) {
        setConfirmedPrimaryKeys(prev => ({
          ...prev,
          [tableId]: data.primary_keys
        }));
      } else if (!silent) {
        setPrimaryKeyData({
          schema: schemaName,
          table: tableName,
          existing_keys: data.primary_keys || [],
          detected_keys: data.detected_keys || [],
          has_keys: data.has_primary_keys
        });
        setShowPrimaryKeyModal(true);
      }
    } catch (error) {
      if (!silent) {
        alert(`Failed to check primary keys: ${error.message}`);
      }
    } finally {
      setLoadingPrimaryKeys(prev => {
        const newState = { ...prev };
        delete newState[tableId];
        return newState;
      });
    }
  };

  const confirmPrimaryKeys = (selectedKeys) => {
    const tableId = `${primaryKeyData.schema}.${primaryKeyData.table}`;
    setConfirmedPrimaryKeys(prev => ({
      ...prev,
      [tableId]: selectedKeys
    }));
    setShowPrimaryKeyModal(false);
  };

  const pollJobStatus = async (tableId, dagId, dagRunId) => {
    const intervalId = setInterval(async () => {
      try {
        const response = await fetch(
          `http://localhost:8000/trigger/status/${dagId}/${dagRunId}`,
          {
            method: 'GET',
            headers: { 'Content-Type': 'application/json' }
          }
        );

        if (!response.ok) {
          throw new Error('Failed to get job status');
        }

        const status = await response.json();
        
        // Map Airflow state -> UI status
        let uiStatus = 'processing';
        let message = 'Running...';
        
        if (status.state === 'success') {
          uiStatus = 'success';
          message = 'Completed successfully';
          clearInterval(intervalId);
          // Remove from polling intervals
          setPollingIntervals(prev => {
            const newIntervals = { ...prev };
            delete newIntervals[tableId];
            return newIntervals;
          });
        } else if (status.state === 'failed') {
          uiStatus = 'error';
          message = 'Job failed';
          clearInterval(intervalId);
          setPollingIntervals(prev => {
            const newIntervals = { ...prev };
            delete newIntervals[tableId];
            return newIntervals;
          });
        } else if (status.state === 'running') {
          message = 'Processing data...';
        } else if (status.state === 'queued') {
          message = 'Waiting in queue...';
        }
        
        setIngestionProgress(prev => ({
          ...prev,
          [tableId]: {
            ...prev[tableId],
            status: uiStatus,
            message: message,
            state: status.state,
            start_date: status.start_date,
            end_date: status.end_date
          }
        }));
        
      } catch (error) {
        console.error(`Failed to poll status for ${tableId}:`, error);
        clearInterval(intervalId);
        setPollingIntervals(prev => {
          const newIntervals = { ...prev };
          delete newIntervals[tableId];
          return newIntervals;
        });
      }
    }, 3000); // Poll mỗi 3 giây

    setPollingIntervals(prev => ({
      ...prev,
      [tableId]: intervalId
    }));
  };

  const handleSubmit = async () => {
    const tablesWithoutPK = selectedTables.filter(
      tableId => !confirmedPrimaryKeys[tableId] || confirmedPrimaryKeys[tableId].length === 0
    );

    if (tablesWithoutPK.length > 0) {
      alert(`Please confirm primary keys for all selected tables.\n\nTables missing primary keys:\n${tablesWithoutPK.join('\n')}`);
      return;
    }

    setIsSubmitting(true);
    setShowProgressModal(true);
    
    const initialProgress = {};
    selectedTables.forEach(tableId => {
      initialProgress[tableId] = { status: 'pending', message: 'Waiting...' };
    });
    setIngestionProgress(initialProgress);
    
    let startTime = Date.now();
    let checkCompletedInterval = null;

    try {
      // Trigger ingest cho từng bảng sequentially
      for (const tableId of selectedTables) {
        const [schemaName, tableName] = tableId.split('.');
        const primaryKeys = confirmedPrimaryKeys[tableId];

        setIngestionProgress(prev => ({
          ...prev,
          [tableId]: { status: 'processing', message: 'Triggering ingestion...' }
        }));

        try {
          const payload = {
            connection_name: dbConfig.connection_name,
            source: {
              schema_name: schemaName,
              table_name: tableName,
              primary_keys: primaryKeys
            },
            target: {
              layer: 'bronze',
              path: `s3a://${dbConfig.database}/bronze/${schemaName}/${tableName}`,
              format: 'delta'
            },
            schedule_type: 'on_demand',
            created_by: 'admin'
          };

          const response = await fetch('http://localhost:8000/trigger/ingest', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
          });

          if (!response.ok) {
            const errorData = await response.json();
            throw new Error(errorData.detail || 'Failed to trigger ingestion');
          }

          const result = await response.json();

          setIngestionProgress(prev => ({
            ...prev,
            [tableId]: { 
              status: 'processing', 
              message: `Job queued, waiting for execution...`,
              dag_run_id: result.dag_run_id,
              dag_id: result.dag_id,
              state: result.state || 'queued'
            }
          }));
          
          pollJobStatus(tableId, result.dag_id, result.dag_run_id);
          await new Promise(resolve => setTimeout(resolve, 500));

        } catch (error) {
          setIngestionProgress(prev => ({
            ...prev,
            [tableId]: { 
              status: 'error', 
              message: error.message 
            }
          }));
        }
      }
      
      // ✅ THÊM PHẦN NÀY - SAU VÒNG FOR
      checkCompletedInterval = setInterval(() => {
        const allProgress = Object.values(ingestionProgress);
        
        console.log('Checking completion:', {
          totalTables: selectedTables.length,
          progressCount: allProgress.length,
          statuses: allProgress.map(p => p.status)
        });
        
        if (Date.now() - startTime > 300000) {
          console.log('⏱️ Timeout after 5 minutes');
          clearInterval(checkCompletedInterval);
          setIsSubmitting(false);
          alert('Ingestion timeout. Please check Airflow.');
          return;
        }
        
        const allCompleted = allProgress.every(p => 
          p.status === 'success' || p.status === 'error'
        );
        
        const hasAllTables = selectedTables.every(tableId => 
          ingestionProgress.hasOwnProperty(tableId)
        );
        
        if (allCompleted && hasAllTables && allProgress.length === selectedTables.length) {
          console.log('✅ All completed!');
          clearInterval(checkCompletedInterval);
          setIsSubmitting(false);
          
          const allSuccess = allProgress.every(p => p.status === 'success');
          
          if (allSuccess) {
            const ingestedTablesData = selectedTables.map(tableId => {
              const [schema, table] = tableId.split('.');
              return {
                schema: schema,
                table: table,
                primary_keys: confirmedPrimaryKeys[tableId]
              };
            });
            
            setTimeout(() => {
              alert('All tables ingested successfully! Redirecting to dashboard...');
              
              if (onIngestionComplete) {
                onIngestionComplete(ingestedTablesData);
              }
            }, 500);
            
          } else {
            alert('Some tables failed to ingest. Please check the progress modal.');
          }
        }
      }, 1000);

    } catch (error) {
      alert(`Failed to trigger ingestion: ${error.message}`);
      setIsSubmitting(false);
      if (checkCompletedInterval) {
        clearInterval(checkCompletedInterval);
      }
    }
  }  // ← Đóng hàm handleSubmit
  const filterTables = (tables) => {
    if (!searchTerm) return tables;
    return tables.filter(table => 
      table.toLowerCase().includes(searchTerm.toLowerCase())
    );
  };

  const getTableStats = () => {
    const totalTables = selectedTables.length;
    const tablesWithPK = selectedTables.filter(id => confirmedPrimaryKeys[id]?.length > 0).length;
    const tablesWithoutPK = totalTables - tablesWithPK;
    
    return { totalTables, tablesWithPK, tablesWithoutPK };
  };

  if (isLoadingSchemas) {
    return (
      <div className="bg-white rounded-lg shadow-sm border border-slate-200 p-12">
        <div className="flex flex-col items-center justify-center space-y-4">
          <Loader2 className="w-12 h-12 text-blue-600 animate-spin" />
          <p className="text-slate-600 font-medium">Loading schemas...</p>
        </div>
      </div>
    );
  }

  const { totalTables, tablesWithPK, tablesWithoutPK } = getTableStats();

  return (
    <div className="space-y-6">
      <div className="bg-blue-50 border border-blue-200 rounded-lg p-4">
        <div className="flex items-center space-x-3">
          <Database className="w-5 h-5 text-blue-600" />
          <div>
            <p className="text-sm font-medium text-blue-900">
              Connected: <span className="font-bold">{dbConfig.connection_name}</span>
            </p>
            <p className="text-xs text-blue-700">
              {dbConfig.database} @ {dbConfig.host}:{dbConfig.port}
            </p>
          </div>
        </div>
      </div>

      <div className="bg-white rounded-lg shadow-sm border border-slate-200 p-6">
        <div className="flex items-center justify-between mb-6">
          <div>
            <h3 className="text-lg font-semibold text-slate-900">Available Schemas</h3>
            <p className="text-sm text-slate-500 mt-1">
              {schemas.length} schema{schemas.length !== 1 ? 's' : ''} found
            </p>
          </div>
          <div className="flex items-center space-x-2">
            <div className="relative">
              <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 w-4 h-4 text-slate-400" />
              <input
                type="text"
                value={searchTerm}
                onChange={(e) => setSearchTerm(e.target.value)}
                placeholder="Search tables..."
                className="pl-9 pr-3 py-2 text-sm border border-slate-300 rounded-lg focus:ring-2 focus:ring-blue-500 w-64"
              />
              {searchTerm && (
                <button
                  onClick={() => setSearchTerm('')}
                  className="absolute right-3 top-1/2 transform -translate-y-1/2"
                >
                  <X className="w-4 h-4 text-slate-400 hover:text-slate-600" />
                </button>
              )}
            </div>
          </div>
        </div>

        {schemas.length === 0 ? (
          <div className="text-center py-12">
            <Database className="w-16 h-16 text-slate-300 mx-auto mb-4" />
            <p className="text-slate-500">No schemas found in this database</p>
          </div>
        ) : (
          <div className="space-y-3">
            {schemas.map((schema) => {
              const filteredTables = filterTables(schema.tables);

              return (
                <div key={schema.name} className="border border-slate-200 rounded-lg overflow-hidden">
                  <div className="flex items-center justify-between bg-slate-50 px-4 py-3">
                    <div
                      onClick={() => toggleSchema(schema.name)}
                      className="flex items-center space-x-3 cursor-pointer hover:text-blue-600 transition-colors flex-1"
                    >
                      {schema.expanded ? (
                        <ChevronDown className="w-4 h-4 text-slate-600" />
                      ) : (
                        <ChevronRight className="w-4 h-4 text-slate-600" />
                      )}
                      <Database className="w-5 h-5 text-blue-600" />
                      <span className="font-medium text-slate-900">{schema.name}</span>
                      <span className="text-xs text-slate-500 bg-slate-200 px-2 py-1 rounded-full">
                        {schema.tables.length} tables
                      </span>
                    </div>
                    {schema.expanded && schema.tables.length > 0 && (
                      <button
                        onClick={(e) => {
                          e.stopPropagation();
                          selectAllTablesInSchema(schema.name);
                        }}
                        className="flex items-center space-x-1 text-sm text-blue-600 hover:text-blue-700 font-medium"
                      >
                        <CheckSquare className="w-4 h-4" />
                        <span>Select All</span>
                      </button>
                    )}
                  </div>

                  {schema.expanded && (
                    <div className="bg-white">
                      {filteredTables.length === 0 ? (
                        <div className="px-4 py-8 text-center text-slate-500 text-sm">
                          {searchTerm ? 'No tables match your search' : 'No tables found in this schema'}
                        </div>
                      ) : (
                        <div className="divide-y divide-slate-100 max-h-96 overflow-y-auto">
                          {filteredTables.map((table) => {
                            const tableId = `${schema.name}.${table}`;
                            const isSelected = selectedTables.includes(tableId);
                            const hasPrimaryKeys = confirmedPrimaryKeys[tableId];
                            const isLoadingPK = loadingPrimaryKeys[tableId];
                            
                            return (
                              <div
                                key={table}
                                className={`flex items-center space-x-3 px-4 py-3 hover:bg-slate-50 transition-colors ${
                                  isSelected ? 'bg-blue-50' : ''
                                }`}
                              >
                                <input
                                  type="checkbox"
                                  checked={isSelected}
                                  onChange={() => toggleTableSelection(schema.name, table)}
                                  className="w-4 h-4 text-blue-600 rounded focus:ring-2 focus:ring-blue-500"
                                />
                                <span className="flex-1 text-sm text-slate-700 font-medium">{table}</span>
                                
                                {isSelected && isLoadingPK && (
                                  <span className="text-xs bg-blue-100 text-blue-700 px-2 py-1 rounded flex items-center space-x-1">
                                    <Loader2 className="w-3 h-3 animate-spin" />
                                    <span>Checking PK...</span>
                                  </span>
                                )}
                                
                                {isSelected && !isLoadingPK && !hasPrimaryKeys && (
                                  <button
                                    onClick={() => checkPrimaryKeys(schema.name, table, false)}
                                    className="text-xs bg-orange-100 text-orange-700 px-2 py-1 rounded flex items-center space-x-1 hover:bg-orange-200 transition-colors"
                                  >
                                    <AlertTriangle className="w-3 h-3" />
                                    <span>Set PK</span>
                                  </button>
                                )}
                                
                                {isSelected && !isLoadingPK && hasPrimaryKeys && (
                                  <div className="flex items-center space-x-2">
                                    <span className="text-xs bg-green-100 text-green-700 px-2 py-1 rounded flex items-center space-x-1">
                                      <Key className="w-3 h-3" />
                                      <span>{hasPrimaryKeys.join(', ')}</span>
                                    </span>
                                    <button
                                      onClick={() => checkPrimaryKeys(schema.name, table, false)}
                                      className="text-xs text-blue-600 hover:text-blue-700"
                                      title="Change primary keys"
                                    >
                                      <RefreshCw className="w-3 h-3" />
                                    </button>
                                  </div>
                                )}
                                
                                <button
                                  onClick={() => previewTable(schema.name, table)}
                                  className="p-1 text-blue-600 hover:bg-blue-50 rounded transition-colors"
                                  title="Preview table"
                                >
                                  <Eye className="w-4 h-4" />
                                </button>
                              </div>
                            );
                          })}
                        </div>
                      )}
                    </div>
                  )}
                </div>
              );
            })}
          </div>
        )}
      </div>

      {selectedTables.length > 0 && (
        <div className="bg-white rounded-lg shadow-sm border border-slate-200 p-6">
          <div className="flex items-center justify-between mb-4">
            <h3 className="text-lg font-semibold text-slate-900">
              Selected Tables ({totalTables})
            </h3>
            <div className="flex items-center space-x-4 text-sm">
              <span className="flex items-center space-x-1 text-green-600">
                <Check className="w-4 h-4" />
                <span>{tablesWithPK} with PK</span>
              </span>
              {tablesWithoutPK > 0 && (
                <span className="flex items-center space-x-1 text-orange-600">
                  <AlertTriangle className="w-4 h-4" />
                  <span>{tablesWithoutPK} missing PK</span>
                </span>
              )}
            </div>
          </div>

          <div className="flex flex-wrap gap-2 mb-6">
            {selectedTables.map((tableId) => {
              const hasPrimaryKeys = confirmedPrimaryKeys[tableId];
              const isLoadingPK = loadingPrimaryKeys[tableId];
              
              return (
                <span
                  key={tableId}
                  className={`px-3 py-1.5 rounded-lg text-sm flex items-center space-x-2 ${
                    isLoadingPK
                      ? 'bg-blue-100 text-blue-800'
                      : hasPrimaryKeys 
                      ? 'bg-green-100 text-green-800' 
                      : 'bg-orange-100 text-orange-800'
                  }`}
                >
                  <span className="font-medium">{tableId}</span>
                  {isLoadingPK && <Loader2 className="w-3 h-3 animate-spin" />}
                  {!isLoadingPK && !hasPrimaryKeys && <AlertTriangle className="w-3 h-3" />}
                  {!isLoadingPK && hasPrimaryKeys && <Check className="w-3 h-3" />}
                  <button
                    onClick={() => {
                      const [schema, table] = tableId.split('.');
                      toggleTableSelection(schema, table);
                    }}
                    className="hover:opacity-70 transition-opacity"
                  >
                    <Trash2 className="w-3 h-3" />
                  </button>
                </span>
              );
            })}
          </div>

          {tablesWithoutPK > 0 && (
            <div className="bg-orange-50 border border-orange-200 rounded-lg p-4 mb-6">
              <div className="flex items-start space-x-3">
                <AlertTriangle className="w-5 h-5 text-orange-600 mt-0.5" />
                <div>
                  <h4 className="font-medium text-orange-900">Primary Keys Required</h4>
                  <p className="text-sm text-orange-700 mt-1">
                    {tablesWithoutPK} table{tablesWithoutPK !== 1 ? 's' : ''} need primary keys before submission. 
                    Click "Set PK" button to configure.
                  </p>
                </div>
              </div>
            </div>
          )}

          <div className="flex justify-between items-center">
            <button
              onClick={() => setStep(1)}
              className="text-slate-600 hover:text-slate-900 font-medium transition-colors"
            >
              ← Back to Connection
            </button>
            <button
              onClick={handleSubmit}
              disabled={isSubmitting || tablesWithoutPK > 0}
              className="flex items-center space-x-2 bg-green-600 text-white px-6 py-2.5 rounded-lg hover:bg-green-700 disabled:bg-slate-400 disabled:cursor-not-allowed transition-colors"
            >
              {isSubmitting ? (
                <>
                  <RefreshCw className="w-4 h-4 animate-spin" />
                  <span>Triggering Ingestion...</span>
                </>
              ) : (
                <>
                  <Check className="w-4 h-4" />
                  <span>Trigger Ingestion</span>
                </>
              )}
            </button>
          </div>
        </div>
      )}

      {showPreviewModal && (
        <div className="fixed inset-0 bg-black bg-opacity-65 flex items-center justify-center z-50 p-4">
          <div className="bg-white rounded-lg shadow-xl max-w-6xl w-full max-h-[90vh] overflow-hidden">
            <div className="flex items-center justify-between p-6 border-b">
              <h3 className="text-lg font-semibold text-slate-900">
                Table Preview: {previewData?.schema}.{previewData?.table}
              </h3>
              <button
                onClick={() => setShowPreviewModal(false)}
                className="p-2 hover:bg-slate-100 rounded-lg transition-colors"
              >
                <X className="w-5 h-5" />
              </button>
            </div>
            <div className="p-6 overflow-auto max-h-[calc(90vh-8rem)]">
              {loadingPreview ? (
                <div className="flex items-center justify-center py-12">
                  <Loader2 className="w-8 h-8 text-blue-600 animate-spin" />
                </div>
              ) : previewData ? (
                <div className="overflow-x-auto">
                  <table className="w-full text-sm border-collapse">
                    <thead className="bg-slate-100 sticky top-0">
                      <tr>
                        {previewData.columns.map((col, idx) => (
                          <th key={idx} className="px-4 py-2 text-left font-medium text-slate-700 whitespace-nowrap border-b-2 border-slate-200">
                            {col}
                          </th>
                        ))}
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-slate-200">
                      {previewData.rows.map((row, rowIdx) => (
                        <tr key={rowIdx} className="hover:bg-slate-50">
                          {row.map((cell, cellIdx) => (
                            <td key={cellIdx} className="px-4 py-2 text-slate-600 whitespace-nowrap">
                              {cell !== null ? String(cell) : <span className="text-slate-400 italic">null</span>}
                            </td>
                          ))}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                  <div className="mt-4 text-sm text-slate-500 text-center">
                    Showing {previewData.row_count} rows
                  </div>
                </div>
              ) : null}
            </div>
          </div>
        </div>
      )}
      
      {/* Progress Modal */}
      {showProgressModal && (
        <div className="fixed inset-0 bg-black bg-opacity-65 flex items-center justify-center z-50 p-4">
          <div className="bg-white rounded-lg shadow-xl max-w-3xl w-full max-h-[80vh] overflow-hidden">
            <div className="flex items-center justify-between p-6 border-b">
              <h3 className="text-lg font-semibold text-slate-900">
                Ingestion Progress
              </h3>
              <button
                onClick={() => setShowProgressModal(false)}
                disabled={isSubmitting}
                className="p-2 hover:bg-slate-100 rounded-lg transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
              >
                <X className="w-5 h-5" />
              </button>
            </div>
            
            <div className="p-6 overflow-auto max-h-[calc(80vh-12rem)]">
              <div className="space-y-3">
                {selectedTables.map(tableId => {
                  const progress = ingestionProgress[tableId] || { status: 'pending', message: 'Waiting...' };
                  
                  return (
                    <div
                      key={tableId}
                      className={`p-4 rounded-lg border-2 transition-all ${
                        progress.status === 'success' ? 'border-green-200 bg-green-50' :
                        progress.status === 'error' ? 'border-red-200 bg-red-50' :
                        progress.status === 'processing' ? 'border-blue-200 bg-blue-50' :
                        'border-slate-200 bg-slate-50'
                      }`}
                    >
                      <div className="flex items-center justify-between">
                        <div className="flex items-center space-x-3 flex-1">
                          {progress.status === 'success' && <Check className="w-5 h-5 text-green-600 flex-shrink-0" />}
                          {progress.status === 'error' && <AlertTriangle className="w-5 h-5 text-red-600 flex-shrink-0" />}
                          {progress.status === 'processing' && <Loader2 className="w-5 h-5 text-blue-600 animate-spin flex-shrink-0" />}
                          {progress.status === 'pending' && <div className="w-5 h-5 rounded-full border-2 border-slate-300 flex-shrink-0" />}
                          
                          <div className="flex-1 min-w-0">
                            <p className="font-medium text-slate-900 truncate">{tableId}</p>
                            <p className="text-sm text-slate-600">{progress.message}</p>
                            {progress.dag_run_id && (
                              <p className="text-xs text-slate-500 mt-1 font-mono">
                                Run ID: {progress.dag_run_id}
                              </p>
                            )}
                            {/* ✅ THÊM: Hiển thị Airflow state */}
                            {progress.state && (
                              <p className="text-xs text-slate-500 mt-1">
                                Airflow State: <span className="font-semibold uppercase">{progress.state}</span>
                              </p>
                            )}
                            {/* ✅ THÊM: Hiển thị start_date nếu có */}
                            {progress.start_date && (
                              <p className="text-xs text-slate-400 mt-1">
                                Started: {new Date(progress.start_date).toLocaleString()}
                              </p>
                            )}
                          </div>
                        </div>
                        
                        <span className={`text-xs px-3 py-1 rounded-full font-medium flex-shrink-0 ml-3 ${
                          progress.status === 'success' ? 'bg-green-100 text-green-700' :
                          progress.status === 'error' ? 'bg-red-100 text-red-700' :
                          progress.status === 'processing' ? 'bg-blue-100 text-blue-700' :
                          'bg-slate-100 text-slate-700'
                        }`}>
                          {progress.status.toUpperCase()}
                        </span>
                      </div>
                    </div>
                  );
                })}
              </div>
            </div>
            
            <div className="p-6 border-t bg-slate-50">
              <div className="flex justify-between items-center">
                <div className="text-sm text-slate-600">
                  <span className="font-semibold">
                    {Object.values(ingestionProgress).filter(p => p.status === 'success').length}
                  </span>
                  {' / '}
                  <span>{selectedTables.length}</span>
                  {' completed'}
                </div>
                <button
                  onClick={() => setShowProgressModal(false)}
                  disabled={isSubmitting}
                  className="px-6 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:bg-slate-400 disabled:cursor-not-allowed transition-colors font-medium"
                >
                  {isSubmitting ? 'Processing...' : 'Close'}
                </button>
              </div>
            </div>
          </div>
        </div>
      )}

      {showPrimaryKeyModal && primaryKeyData && (
        <PrimaryKeyConfirmModal
          data={primaryKeyData}
          onConfirm={confirmPrimaryKeys}
          onClose={() => setShowPrimaryKeyModal(false)}
        />
      )}
    </div>
  );
}

function PrimaryKeyConfirmModal({ data, onConfirm, onClose }) {
  const [selectedKeys, setSelectedKeys] = useState(
    data.existing_keys.length > 0 ? data.existing_keys : data.detected_keys
  );

  const toggleKey = (key) => {
    if (selectedKeys.includes(key)) {
      setSelectedKeys(selectedKeys.filter(k => k !== key));
    } else {
      setSelectedKeys([...selectedKeys, key]);
    }
  };

  const allKeys = [...new Set([...data.existing_keys, ...data.detected_keys])];

  return (
    <div className="fixed inset-0 bg-black bg-opacity-65 flex items-center justify-center z-50 p-4">
      <div className="bg-white rounded-lg shadow-xl max-w-2xl w-full">
        <div className="p-6 border-b">
          <h3 className="text-lg font-semibold text-slate-900">
            Confirm Primary Keys
          </h3>
          <p className="text-sm text-slate-500 mt-1">
            {data.schema}.{data.table}
          </p>
        </div>
        
        <div className="p-6 space-y-4">
          {!data.has_keys && (
            <div className="bg-orange-50 border border-orange-200 rounded-lg p-4">
              <div className="flex items-start space-x-3">
                <AlertTriangle className="w-5 h-5 text-orange-600 mt-0.5" />
                <div>
                  <h4 className="font-medium text-orange-900">No Primary Keys Found</h4>
                  <p className="text-sm text-orange-700 mt-1">
                    This table has no defined primary keys. We've detected potential keys based on uniqueness.
                  </p>
                </div>
              </div>
            </div>
          )}

          <div className="space-y-2">
            <label className="block text-sm font-medium text-slate-700">
              Select Primary Key Columns:
            </label>
            {allKeys.length > 0 ? (
              allKeys.map((key) => {
                const isExisting = data.existing_keys.includes(key);
                const isDetected = data.detected_keys.includes(key);
                const isSelected = selectedKeys.includes(key);

                return (
                  <label
                    key={key}
                    className="flex items-center space-x-3 p-3 border border-slate-200 rounded-lg hover:bg-slate-50 cursor-pointer transition-colors"
                  >
                    <input
                      type="checkbox"
                      checked={isSelected}
                      onChange={() => toggleKey(key)}
                      className="w-4 h-4 text-blue-600 rounded"
                    />
                    <span className="flex-1 font-mono text-sm font-medium">{key}</span>
                    {isExisting && (
                      <span className="text-xs bg-green-100 text-green-700 px-2 py-1 rounded font-medium">
                        Existing PK
                      </span>
                    )}
                    {!isExisting && isDetected && (
                      <span className="text-xs bg-blue-100 text-blue-700 px-2 py-1 rounded font-medium">
                        Detected
                      </span>
                    )}
                  </label>
                );
              })
            ) : (
              <p className="text-sm text-slate-500 italic p-4 text-center bg-slate-50 rounded-lg">
                No suitable primary key columns found
              </p>
            )}
          </div>
        </div>

        <div className="p-6 border-t flex justify-end space-x-3">
          <button
            onClick={onClose}
            className="px-4 py-2 text-slate-600 hover:text-slate-900 font-medium transition-colors"
          >
            Cancel
          </button>
          <button
            onClick={() => onConfirm(selectedKeys)}
            disabled={selectedKeys.length === 0}
            className="px-6 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:bg-slate-300 disabled:cursor-not-allowed transition-colors font-medium"
          >
            Confirm Selection
          </button>
        </div>
      </div>
    </div>
  );
}