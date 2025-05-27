import React from 'react';
import ConnectionCard from './ConnectionCard';
import { Connection } from '../api/connections';

interface ConnectionListProps {
  connections: Connection[];
  onDelete: (id: string) => void;
  onRefresh: () => void;
}

const ConnectionList: React.FC<ConnectionListProps> = ({ connections, onDelete, onRefresh }) => {
  if (connections.length === 0) {
    return (
      <div className="empty-state">
        <p>No connections yet. Create your first Snowflake connection!</p>
      </div>
    );
  }

  return (
    <div className="connections-section">
      <div className="section-header">
        <h2>Connections</h2>
        <button className="btn btn-secondary" onClick={onRefresh}>
          🔄 Refresh
        </button>
      </div>
      
      <div className="connections-grid">
        {connections.map(connection => (
          <ConnectionCard
            key={connection.id}
            connection={connection}
            onDelete={onDelete}
          />
        ))}
      </div>
    </div>
  );
};

export default ConnectionList;
