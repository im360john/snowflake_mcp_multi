import React, { useState, useEffect } from 'react';
import ConnectionList from './components/ConnectionList';
import ConnectionForm from './components/ConnectionForm';
import { Connection, createConnection, listConnections, deleteConnection } from './api/connections';
import './App.css';

function App() {
  const [connections, setConnections] = useState<Connection[]>([]);
  const [showForm, setShowForm] = useState(false);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    loadConnections();
  }, []);

  const loadConnections = async () => {
    try {
      const data = await listConnections();
      setConnections(data);
    } catch (error) {
      console.error('Failed to load connections:', error);
    } finally {
      setLoading(false);
    }
  };

  const handleCreateConnection = async (connectionData: any) => {
    try {
      const newConnection = await createConnection(connectionData);
      setConnections([...connections, newConnection]);
      setShowForm(false);
    } catch (error) {
      console.error('Failed to create connection:', error);
      alert('Failed to create connection');
    }
  };

  const handleDeleteConnection = async (id: string) => {
    try {
      await deleteConnection(id);
      setConnections(connections.filter(conn => conn.id !== id));
    } catch (error) {
      console.error('Failed to delete connection:', error);
    }
  };

  return (
    <div className="app">
      <header className="app-header">
        <h1>🏔️ Snowflake MCP Manager</h1>
        <p>Universal MCP Endpoint for Your Snowflake Data</p>
      </header>

      <main className="app-main">
        <div className="actions">
          <button 
            className="btn btn-primary"
            onClick={() => setShowForm(!showForm)}
          >
            {showForm ? 'Cancel' : '➕ Add New Connection'}
          </button>
        </div>

        {showForm && (
          <ConnectionForm 
            onSubmit={handleCreateConnection}
            onCancel={() => setShowForm(false)}
          />
        )}

        {loading ? (
          <div className="loading">Loading connections...</div>
        ) : (
          <ConnectionList 
            connections={connections}
            onDelete={handleDeleteConnection}
            onRefresh={loadConnections}
          />
        )}
      </main>
    </div>
  );
}

export default App;
