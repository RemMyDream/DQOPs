// SchemaTableSelector.jsx
import React, { useState } from 'react';
import { Plus, Trash2, Database, Check, ChevronDown, ChevronRight } from 'lucide-react';

export default function SchemaTableSelector({
  schemas,
  setSchemas,
  selectedTables,
  setSelectedTables,
  setStep,
  submitSelectedTables
}) {
  const [newSchemaName, setNewSchemaName] = useState('');

  const toggleSchema = (schemaName) => {
    setSchemas(schemas.map(schema => 
      schema.name === schemaName 
        ? { ...schema, expanded: !schema.expanded }
        : schema
    ));
  };

  const toggleTableSelection = (schemaName, tableName) => {
    const tableId = `${schemaName}.${tableName}`;
    if (selectedTables.includes(tableId)) {
      setSelectedTables(selectedTables.filter(id => id !== tableId));
    } else {
      setSelectedTables([...selectedTables, tableId]);
    }
  };

  const addNewSchema = () => {
    if (newSchemaName && !schemas.find(s => s.name === newSchemaName)) {
      setSchemas([...schemas, {
        name: newSchemaName,
        expanded: true,
        tables: []
      }]);
      setNewSchemaName('');
    }
  };

  return (
    <div className="space-y-6">
      <div className="bg-white rounded-lg shadow-sm border border-slate-200 p-6">
        <div className="flex items-center justify-between mb-6">
          <h3 className="text-lg font-semibold text-slate-900">Available Schemas</h3>
          <div className="flex items-center space-x-2">
            <input
              type="text"
              value={newSchemaName}
              onChange={(e) => setNewSchemaName(e.target.value)}
              onKeyPress={(e) => e.key === 'Enter' && addNewSchema()}
              placeholder="New schema name..."
              className="px-3 py-2 text-sm border border-slate-300 rounded-lg focus:ring-2 focus:ring-blue-500"
            />
            <button
              onClick={addNewSchema}
              disabled={!newSchemaName}
              className="flex items-center space-x-1 bg-blue-600 text-white px-4 py-2 rounded-lg hover:bg-blue-700 disabled:bg-slate-300 text-sm"
            >
              <Plus className="w-4 h-4" />
              <span>Add Schema</span>
            </button>
          </div>
        </div>

        <div className="space-y-3">
          {schemas.map((schema) => (
            <div key={schema.name} className="border border-slate-200 rounded-lg overflow-hidden">
              {/* Schema Header */}
              <div
                onClick={() => toggleSchema(schema.name)}
                className="flex items-center justify-between bg-slate-50 px-4 py-3 cursor-pointer hover:bg-slate-100 transition-colors"
              >
                <div className="flex items-center space-x-3">
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
              </div>

              {/* Tables List */}
              {schema.expanded && (
                <div className="bg-white">
                  {schema.tables.length === 0 ? (
                    <div className="px-4 py-8 text-center text-slate-500 text-sm">
                      No tables found in this schema
                    </div>
                  ) : (
                    <div className="divide-y divide-slate-100">
                      {schema.tables.map((table) => {
                        const tableId = `${schema.name}.${table}`;
                        const isSelected = selectedTables.includes(tableId);
                        
                        return (
                          <label
                            key={table}
                            className="flex items-center space-x-3 px-4 py-3 hover:bg-slate-50 cursor-pointer transition-colors"
                          >
                            <input
                              type="checkbox"
                              checked={isSelected}
                              onChange={() => toggleTableSelection(schema.name, table)}
                              className="w-4 h-4 text-blue-600 rounded focus:ring-2 focus:ring-blue-500"
                            />
                            <span className="flex-1 text-sm text-slate-700">{table}</span>
                            {isSelected && (
                              <span className="text-xs bg-green-100 text-green-700 px-2 py-1 rounded-full">
                                Selected
                              </span>
                            )}
                          </label>
                        );
                      })}
                    </div>
                  )}
                </div>
              )}
            </div>
          ))}
        </div>
      </div>

      {/* Selected Tables Summary */}
      {selectedTables.length > 0 && (
        <div className="bg-white rounded-lg shadow-sm border border-slate-200 p-6">
          <h3 className="text-lg font-semibold text-slate-900 mb-4">
            Selected Tables ({selectedTables.length})
          </h3>
          <div className="flex flex-wrap gap-2 mb-6">
            {selectedTables.map((tableId) => (
              <span
                key={tableId}
                className="bg-blue-100 text-blue-800 px-3 py-1 rounded-full text-sm flex items-center space-x-2"
              >
                <span>{tableId}</span>
                <button
                  onClick={() => {
                    const [schema, table] = tableId.split('.');
                    toggleTableSelection(schema, table);
                  }}
                  className="text-blue-600 hover:text-blue-800"
                >
                  <Trash2 className="w-3 h-3" />
                </button>
              </span>
            ))}
          </div>

          <div className="flex justify-between items-center">
            <button
              onClick={() => setStep(1)}
              className="text-slate-600 hover:text-slate-900 font-medium"
            >
              ← Back to Connection
            </button>
            <button
              onClick={submitSelectedTables}
              className="flex items-center space-x-2 bg-green-600 text-white px-6 py-2.5 rounded-lg hover:bg-green-700 transition-colors"
            >
              <Check className="w-4 h-4" />
              <span>Add to Data Quality System</span>
            </button>
          </div>
        </div>
      )}
    </div>
  );
}