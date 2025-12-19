import React, { useState, useEffect } from 'react';
import { 
  Database, 
  ChevronRight, 
  ChevronDown,
  Table,
  Columns,
  BarChart3,
  TrendingUp,
  AlertCircle,
  RefreshCw,
  Settings,
  LogOut
} from 'lucide-react';

export default function DataQualityDashboard({ 
  connectionName,
  ingestedTables, // Array of {schema, table, primary_keys}
  onLogout 
}) {
  const [connections, setConnections] = useState([]);
  const [selectedConnection, setSelectedConnection] = useState(null);
  const [selectedSchema, setSelectedSchema] = useState(null);
  const [selectedTable, setSelectedTable] = useState(null);
  const [selectedColumn, setSelectedColumn] = useState(null);
  const [expandedConnections, setExpandedConnections] = useState({});
  const [expandedSchemas, setExpandedSchemas] = useState({});
  const [expandedTables, setExpandedTables] = useState({});
  const [isLoading, setIsLoading] = useState(true);

  useEffect(() => {
    loadIngestedTables();
  }, [ingestedTables]);

  const loadIngestedTables = async () => {
    setIsLoading(true);
    try {
      if (!ingestedTables || ingestedTables.length === 0) {
        setConnections([]);
        setIsLoading(false);
        return;
      }

      // Group tables by schema
      const schemaMap = {};
      ingestedTables.forEach(({ schema, table }) => {
        if (!schemaMap[schema]) {
          schemaMap[schema] = [];
        }
        schemaMap[schema].push(table);
      });

      // For each schema/table, fetch columns
      const schemasWithColumns = await Promise.all(
        Object.entries(schemaMap).map(async ([schemaName, tables]) => {
          const tablesWithColumns = await Promise.all(
            tables.map(async (tableName) => {
              try {
                const columnsResponse = await fetch(
                  'http://localhost:8000/postgres/tables/columns',
                  {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                      connection_name: connectionName,
                      schema_name: schemaName,
                      table_name: tableName
                    })
                  }
                );

                if (!columnsResponse.ok) {
                  throw new Error(`Failed to fetch columns for ${tableName}`);
                }

                const columnsData = await columnsResponse.json();
                
                return {
                  table_name: tableName,
                  columns: columnsData.columns.map(col => col.column_name)
                };
              } catch (error) {
                console.error(`Error fetching columns for ${tableName}:`, error);
                return {
                  table_name: tableName,
                  columns: []
                };
              }
            })
          );

          return {
            schema_name: schemaName,
            tables: tablesWithColumns
          };
        })
      );

      const connectionData = [{
        connection_name: connectionName,
        schemas: schemasWithColumns
      }];
      
      setConnections(connectionData);
      
      // Auto-expand the connection
      setExpandedConnections({ [connectionName]: true });
      
      // Auto-select the connection
      setSelectedConnection(connectionName);
      
    } catch (error) {
      console.error('Failed to load ingested tables:', error);
      alert(`Failed to load ingested tables: ${error.message}`);
    } finally {
      setIsLoading(false);
    }
  };

  const toggleConnection = (connName) => {
    setExpandedConnections(prev => ({
      ...prev,
      [connName]: !prev[connName]
    }));
  };

  const toggleSchema = (connName, schemaName) => {
    const key = `${connName}.${schemaName}`;
    setExpandedSchemas(prev => ({
      ...prev,
      [key]: !prev[key]
    }));
  };

  const toggleTable = (connName, schemaName, tableName) => {
    const key = `${connName}.${schemaName}.${tableName}`;
    setExpandedTables(prev => ({
      ...prev,
      [key]: !prev[key]
    }));
  };

  const selectConnection = (connName) => {
    setSelectedConnection(connName);
    setSelectedSchema(null);
    setSelectedTable(null);
    setSelectedColumn(null);
  };

  const selectSchema = (connName, schemaName) => {
    setSelectedConnection(connName);
    setSelectedSchema(schemaName);
    setSelectedTable(null);
    setSelectedColumn(null);
  };

  const selectTable = (connName, schemaName, tableName) => {
    setSelectedConnection(connName);
    setSelectedSchema(schemaName);
    setSelectedTable(tableName);
    setSelectedColumn(null);
  };

  const selectColumn = (connName, schemaName, tableName, columnName) => {
    setSelectedConnection(connName);
    setSelectedSchema(schemaName);
    setSelectedTable(tableName);
    setSelectedColumn(columnName);
  };

  const getBreadcrumb = () => {
    const parts = [];
    if (selectedConnection) parts.push(selectedConnection);
    if (selectedSchema) parts.push(selectedSchema);
    if (selectedTable) parts.push(selectedTable);
    if (selectedColumn) parts.push(selectedColumn);
    return parts.join(' / ');
  };

  return (
    <div className="min-h-screen bg-gradient-to-br from-slate-50 to-slate-100 flex flex-col">
      {/* Navbar */}
      <nav className="bg-white shadow-sm border-b border-slate-200">
        <div className="px-6 py-4">
          <div className="flex items-center justify-between">
            <div className="flex items-center space-x-4">
              <div className="flex items-center space-x-3">
                <div className="bg-blue-600 p-2 rounded-lg">
                  <Database className="w-6 h-6 text-white" />
                </div>
                <div>
                  <h1 className="text-2xl font-bold text-slate-900">DQ.DS</h1>
                  <p className="text-xs text-slate-500">Data Quality Dashboard</p>
                </div>
              </div>

              {/* Navigation Tabs */}
              <div className="flex items-center space-x-1 ml-8">
                <button className="px-4 py-2 text-sm font-medium text-blue-600 bg-blue-50 rounded-lg">
                  Data sources
                </button>
                <button className="px-4 py-2 text-sm font-medium text-slate-600 hover:text-slate-900 hover:bg-slate-100 rounded-lg transition-colors">
                  Profiling
                </button>
                <button className="px-4 py-2 text-sm font-medium text-slate-600 hover:text-slate-900 hover:bg-slate-100 rounded-lg transition-colors">
                  Monitoring checks
                </button>
                <button className="px-4 py-2 text-sm font-medium text-slate-600 hover:text-slate-900 hover:bg-slate-100 rounded-lg transition-colors">
                  Partition checks
                </button>
                <button className="px-4 py-2 text-sm font-medium text-slate-600 hover:text-slate-900 hover:bg-slate-100 rounded-lg transition-colors">
                  Data quality dashboards
                </button>
                <button className="px-4 py-2 text-sm font-medium text-slate-600 hover:text-slate-900 hover:bg-slate-100 rounded-lg transition-colors">
                  Incidents
                </button>
                <button className="px-4 py-2 text-sm font-medium text-slate-600 hover:text-slate-900 hover:bg-slate-100 rounded-lg transition-colors">
                  Configuration
                </button>
              </div>
            </div>

            <div className="flex items-center space-x-4">
              <button className="px-4 py-2 text-sm font-medium text-slate-700 hover:text-slate-900 flex items-center space-x-2 bg-white border border-slate-300 rounded-lg hover:bg-slate-50 transition-colors">
                <span>Root data domain</span>
                <ChevronRight className="w-4 h-4" />
              </button>
              
              <button className="px-4 py-2 bg-blue-600 text-white text-sm font-medium rounded-lg hover:bg-blue-700 transition-colors flex items-center space-x-2">
                <RefreshCw className="w-4 h-4" />
                <span>Synchronize</span>
              </button>

              <button className="p-2 text-slate-600 hover:text-slate-900 rounded-lg hover:bg-slate-100 transition-colors">
                <Settings className="w-5 h-5" />
              </button>

              {onLogout && (
                <button 
                  onClick={onLogout}
                  className="p-2 text-slate-600 hover:text-slate-900 rounded-lg hover:bg-slate-100 transition-colors"
                  title="Logout"
                >
                  <LogOut className="w-5 h-5" />
                </button>
              )}
            </div>
          </div>
        </div>
      </nav>

      {/* Main Content Area */}
      <div className="flex-1 flex overflow-hidden">
        {/* Sidebar */}
        <aside className="w-80 bg-white border-r border-slate-200 overflow-y-auto">
          <div className="p-4">
            <div className="flex items-center justify-between mb-4">
              <h2 className="text-sm font-semibold text-slate-700 uppercase tracking-wide">
                Data Sources
              </h2>
              <button 
                onClick={loadIngestedTables}
                className="p-1.5 text-slate-500 hover:text-slate-700 hover:bg-slate-100 rounded transition-colors"
                title="Refresh"
              >
                <RefreshCw className="w-4 h-4" />
              </button>
            </div>

            {isLoading ? (
              <div className="flex items-center justify-center py-12">
                <div className="text-center">
                  <RefreshCw className="w-8 h-8 text-blue-600 animate-spin mx-auto mb-2" />
                  <p className="text-sm text-slate-500">Loading connections...</p>
                </div>
              </div>
            ) : connections.length === 0 ? (
              <div className="text-center py-12">
                <Database className="w-12 h-12 text-slate-300 mx-auto mb-3" />
                <p className="text-sm text-slate-500">No connections found</p>
              </div>
            ) : (
              <div className="space-y-1">
                {connections.map((conn) => (
                  <div key={conn.connection_name}>
                    {/* Connection Level */}
                    <div
                      className={`flex items-center space-x-2 px-3 py-2 rounded-lg cursor-pointer transition-colors ${
                        selectedConnection === conn.connection_name && !selectedSchema
                          ? 'bg-blue-50 text-blue-700'
                          : 'hover:bg-slate-50 text-slate-700'
                      }`}
                      onClick={() => {
                        toggleConnection(conn.connection_name);
                        selectConnection(conn.connection_name);
                      }}
                    >
                      {expandedConnections[conn.connection_name] ? (
                        <ChevronDown className="w-4 h-4 flex-shrink-0" />
                      ) : (
                        <ChevronRight className="w-4 h-4 flex-shrink-0" />
                      )}
                      <Database className="w-4 h-4 flex-shrink-0 text-blue-600" />
                      <span className="text-sm font-medium truncate">
                        {conn.connection_name}
                      </span>
                    </div>

                    {/* Schema Level */}
                    {expandedConnections[conn.connection_name] && (
                      <div className="ml-4 mt-1 space-y-1">
                        {conn.schemas.map((schema) => {
                          const schemaKey = `${conn.connection_name}.${schema.schema_name}`;
                          return (
                            <div key={schemaKey}>
                              <div
                                className={`flex items-center space-x-2 px-3 py-2 rounded-lg cursor-pointer transition-colors ${
                                  selectedConnection === conn.connection_name &&
                                  selectedSchema === schema.schema_name &&
                                  !selectedTable
                                    ? 'bg-blue-50 text-blue-700'
                                    : 'hover:bg-slate-50 text-slate-600'
                                }`}
                                onClick={() => {
                                  toggleSchema(conn.connection_name, schema.schema_name);
                                  selectSchema(conn.connection_name, schema.schema_name);
                                }}
                              >
                                {expandedSchemas[schemaKey] ? (
                                  <ChevronDown className="w-3.5 h-3.5 flex-shrink-0" />
                                ) : (
                                  <ChevronRight className="w-3.5 h-3.5 flex-shrink-0" />
                                )}
                                <Database className="w-3.5 h-3.5 flex-shrink-0" />
                                <span className="text-sm truncate">{schema.schema_name}</span>
                                <span className="text-xs text-slate-400 ml-auto">
                                  {schema.tables.length}
                                </span>
                              </div>

                              {/* Table Level */}
                              {expandedSchemas[schemaKey] && (
                                <div className="ml-4 mt-1 space-y-1">
                                  {schema.tables.map((table) => {
                                    const tableKey = `${conn.connection_name}.${schema.schema_name}.${table.table_name}`;
                                    return (
                                      <div key={tableKey}>
                                        <div
                                          className={`flex items-center space-x-2 px-3 py-2 rounded-lg cursor-pointer transition-colors ${
                                            selectedConnection === conn.connection_name &&
                                            selectedSchema === schema.schema_name &&
                                            selectedTable === table.table_name &&
                                            !selectedColumn
                                              ? 'bg-blue-50 text-blue-700'
                                              : 'hover:bg-slate-50 text-slate-600'
                                          }`}
                                          onClick={() => {
                                            toggleTable(
                                              conn.connection_name,
                                              schema.schema_name,
                                              table.table_name
                                            );
                                            selectTable(
                                              conn.connection_name,
                                              schema.schema_name,
                                              table.table_name
                                            );
                                          }}
                                        >
                                          {expandedTables[tableKey] ? (
                                            <ChevronDown className="w-3 h-3 flex-shrink-0" />
                                          ) : (
                                            <ChevronRight className="w-3 h-3 flex-shrink-0" />
                                          )}
                                          <Table className="w-3 h-3 flex-shrink-0" />
                                          <span className="text-sm truncate">
                                            {table.table_name}
                                          </span>
                                          <span className="text-xs text-slate-400 ml-auto">
                                            {table.columns.length}
                                          </span>
                                        </div>

                                        {/* Column Level */}
                                        {expandedTables[tableKey] && (
                                          <div className="ml-4 mt-1 space-y-1">
                                            {table.columns.map((column) => (
                                              <div
                                                key={column}
                                                className={`flex items-center space-x-2 px-3 py-1.5 rounded-lg cursor-pointer transition-colors ${
                                                  selectedConnection === conn.connection_name &&
                                                  selectedSchema === schema.schema_name &&
                                                  selectedTable === table.table_name &&
                                                  selectedColumn === column
                                                    ? 'bg-blue-50 text-blue-700'
                                                    : 'hover:bg-slate-50 text-slate-500'
                                                }`}
                                                onClick={() =>
                                                  selectColumn(
                                                    conn.connection_name,
                                                    schema.schema_name,
                                                    table.table_name,
                                                    column
                                                  )
                                                }
                                              >
                                                <Columns className="w-3 h-3 flex-shrink-0" />
                                                <span className="text-xs truncate">{column}</span>
                                              </div>
                                            ))}
                                          </div>
                                        )}
                                      </div>
                                    );
                                  })}
                                </div>
                              )}
                            </div>
                          );
                        })}
                      </div>
                    )}
                  </div>
                ))}
              </div>
            )}
          </div>
        </aside>

        {/* Main Content */}
        <main className="flex-1 overflow-y-auto bg-gradient-to-br from-slate-50 to-slate-100">
          <div className="p-6">
            {/* Breadcrumb */}
            {getBreadcrumb() && (
              <div className="mb-6">
                <div className="flex items-center space-x-2 text-sm text-slate-600">
                  <span>Data sources</span>
                  {getBreadcrumb().split(' / ').map((part, index, array) => (
                    <React.Fragment key={index}>
                      <ChevronRight className="w-4 h-4" />
                      <span className={index === array.length - 1 ? 'font-semibold text-slate-900' : ''}>
                        {part}
                      </span>
                    </React.Fragment>
                  ))}
                </div>
              </div>
            )}

            {/* Content Area */}
            {!selectedConnection && (
              <div className="text-center py-20">
                <Database className="w-20 h-20 text-slate-300 mx-auto mb-4" />
                <h3 className="text-xl font-semibold text-slate-700 mb-2">
                  Select a data source
                </h3>
                <p className="text-slate-500">
                  Choose a connection, schema, table, or column from the sidebar to view details
                </p>
              </div>
            )}

            {selectedConnection && !selectedSchema && (
              <div className="bg-white rounded-lg shadow-sm border border-slate-200 p-8">
                <div className="flex items-center space-x-4 mb-6">
                  <div className="p-3 bg-blue-100 rounded-lg">
                    <Database className="w-8 h-8 text-blue-600" />
                  </div>
                  <div>
                    <h2 className="text-2xl font-bold text-slate-900">{selectedConnection}</h2>
                    <p className="text-slate-500">Database Connection</p>
                  </div>
                </div>
                <div className="grid grid-cols-3 gap-4">
                  <div className="bg-slate-50 rounded-lg p-4">
                    <p className="text-sm text-slate-600 mb-1">Schemas</p>
                    <p className="text-2xl font-bold text-slate-900">
                      {connections.find(c => c.connection_name === selectedConnection)?.schemas.length || 0}
                    </p>
                  </div>
                  <div className="bg-slate-50 rounded-lg p-4">
                    <p className="text-sm text-slate-600 mb-1">Tables</p>
                    <p className="text-2xl font-bold text-slate-900">
                      {connections.find(c => c.connection_name === selectedConnection)?.schemas.reduce(
                        (acc, s) => acc + s.tables.length, 0
                      ) || 0}
                    </p>
                  </div>
                  <div className="bg-slate-50 rounded-lg p-4">
                    <p className="text-sm text-slate-600 mb-1">Status</p>
                    <p className="text-sm font-semibold text-green-600">Connected</p>
                  </div>
                </div>
              </div>
            )}

            {selectedConnection && selectedSchema && !selectedTable && (
              <div className="bg-white rounded-lg shadow-sm border border-slate-200 p-8">
                <div className="flex items-center space-x-4 mb-6">
                  <div className="p-3 bg-purple-100 rounded-lg">
                    <Database className="w-8 h-8 text-purple-600" />
                  </div>
                  <div>
                    <h2 className="text-2xl font-bold text-slate-900">{selectedSchema}</h2>
                    <p className="text-slate-500">{selectedConnection} / Schema</p>
                  </div>
                </div>
                <div className="grid grid-cols-2 gap-4">
                  <div className="bg-slate-50 rounded-lg p-4">
                    <p className="text-sm text-slate-600 mb-1">Tables</p>
                    <p className="text-2xl font-bold text-slate-900">
                      {connections.find(c => c.connection_name === selectedConnection)
                        ?.schemas.find(s => s.schema_name === selectedSchema)
                        ?.tables.length || 0}
                    </p>
                  </div>
                  <div className="bg-slate-50 rounded-lg p-4">
                    <p className="text-sm text-slate-600 mb-1">Total Columns</p>
                    <p className="text-2xl font-bold text-slate-900">
                      {connections.find(c => c.connection_name === selectedConnection)
                        ?.schemas.find(s => s.schema_name === selectedSchema)
                        ?.tables.reduce((acc, t) => acc + t.columns.length, 0) || 0}
                    </p>
                  </div>
                </div>
              </div>
            )}

            {selectedConnection && selectedSchema && selectedTable && !selectedColumn && (
              <div className="bg-white rounded-lg shadow-sm border border-slate-200 overflow-hidden">
                <div className="p-6 border-b border-slate-200">
                  <div className="flex items-center space-x-4 mb-4">
                    <div className="p-3 bg-green-100 rounded-lg">
                      <Table className="w-8 h-8 text-green-600" />
                    </div>
                    <div>
                      <h2 className="text-2xl font-bold text-slate-900">{selectedTable}</h2>
                      <p className="text-slate-500">{selectedConnection} / {selectedSchema} / Table</p>
                    </div>
                  </div>
                  
                  <button className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors font-medium text-sm">
                    Collect statistics
                  </button>
                </div>

                {/* Column Statistics Table Placeholder */}
                <div className="p-6">
                  <h3 className="text-lg font-semibold text-slate-900 mb-4">Columns</h3>
                  <div className="overflow-x-auto">
                    <table className="w-full text-sm">
                      <thead className="bg-slate-50">
                        <tr>
                          <th className="px-4 py-3 text-left font-medium text-slate-700">Column name</th>
                          <th className="px-4 py-3 text-left font-medium text-slate-700">Data type</th>
                          <th className="px-4 py-3 text-left font-medium text-slate-700">Nulls</th>
                          <th className="px-4 py-3 text-left font-medium text-slate-700">Distinct</th>
                          <th className="px-4 py-3 text-left font-medium text-slate-700">Action</th>
                        </tr>
                      </thead>
                      <tbody className="divide-y divide-slate-200">
                        {connections.find(c => c.connection_name === selectedConnection)
                          ?.schemas.find(s => s.schema_name === selectedSchema)
                          ?.tables.find(t => t.table_name === selectedTable)
                          ?.columns.map((column) => (
                            <tr key={column} className="hover:bg-slate-50">
                              <td className="px-4 py-3">
                                <button
                                  onClick={() => selectColumn(selectedConnection, selectedSchema, selectedTable, column)}
                                  className="text-blue-600 hover:text-blue-700 font-medium"
                                >
                                  {column}
                                </button>
                              </td>
                              <td className="px-4 py-3 text-slate-600">-</td>
                              <td className="px-4 py-3 text-slate-600">-</td>
                              <td className="px-4 py-3 text-slate-600">-</td>
                              <td className="px-4 py-3">
                                <button className="p-1.5 text-blue-600 hover:bg-blue-50 rounded transition-colors">
                                  <BarChart3 className="w-4 h-4" />
                                </button>
                              </td>
                            </tr>
                          ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              </div>
            )}

            {selectedConnection && selectedSchema && selectedTable && selectedColumn && (
              <div className="bg-white rounded-lg shadow-sm border border-slate-200 p-8">
                <div className="flex items-center space-x-4 mb-6">
                  <div className="p-3 bg-indigo-100 rounded-lg">
                    <Columns className="w-8 h-8 text-indigo-600" />
                  </div>
                  <div>
                    <h2 className="text-2xl font-bold text-slate-900">{selectedColumn}</h2>
                    <p className="text-slate-500">
                      {selectedConnection} / {selectedSchema} / {selectedTable} / Column
                    </p>
                  </div>
                </div>
                
                <div className="bg-blue-50 border border-blue-200 rounded-lg p-6 text-center">
                  <BarChart3 className="w-12 h-12 text-blue-600 mx-auto mb-3" />
                  <p className="text-slate-600">
                    Column statistics will be displayed here after collecting data quality metrics
                  </p>
                </div>
              </div>
            )}
          </div>
        </main>
      </div>
    </div>
  );
}