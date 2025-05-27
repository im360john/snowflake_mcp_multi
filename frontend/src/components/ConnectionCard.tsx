import React, { useState } from 'react';
import { Connection, startConnection, stopConnection } from '../api/connections';

interface ConnectionCardProps {
  connection: Connection;
  onDelete: (id: string) => void;
}

const ConnectionCard: React.FC<ConnectionCardProps> = ({ connection, onDelete }) => {
  const [isActive, setIsActive] = useState(connection.active);
  const [loading, setLoading] = useState(false);
  const [sseEndpoint, setSseEndpoint] = useState(connection.sse_endpoint);

  const handleToggle = async () => {
    setLoading(true);
    try {
      if (isActive) {
        await stopConnection(connection.id);
        setIsActive(false);
      } else {
        const result = await startConnection(connection.id);
        setIsActive(true);
        setSseEndpoint(result.sse_endpoint);
      }
    } catch (error) {
      console.error('Failed to toggle connection:', error);
    } finally {
      setLoading(false);
    }
  };

  const copyEndpoint = () => {
    navigator.clipboard.writeText(sseEndpoint);
    alert('SSE endpoint copied to clipboard!');
  };

  return (
    <div className={`connection-card ${isActive ? 'active' : ''}`}>
      <div className="connection-header">
        <div className="connection-icon">❄️</div>
        <div className="connection-info">
          <h3>{connection.name}</h3>
          <p className="connection-type">Snowflake</p>
        </div>
        <button 
          className="btn-menu"
          onClick={() => onDelete(connection.id)}
        >
          ⋮
        </button>
      </div>

      <div className="connection-details">
        <div className="detail-row">
          <span className="label">Account:</span>
          <span className="value">{connection.account}</span>
        </div>
        <div className="detail-row">
          <span className="label">Status:</span>
          <span className={`status ${isActive ? 'active' : 'inactive'}`}>
            {isActive ? '🟢 Active' : '⚪ Inactive'}
          </span>
        </div>
      </div>

      {isActive && sseEndpoint && (
        <div className="endpoint-section">
          <div className="endpoint-header">
            <span className="label">SSE Endpoint:</span>
            <button className="btn-copy" onClick={copyEndpoint}>📋</button>
          </div>
          <input 
            type="text" 
            value={sseEndpoint} 
            readOnly 
            className="endpoint-input"
          />
        </div>
      )}

      <div className="connection-actions">
        <button 
          className={`btn ${isActive ? 'btn-danger' : 'btn-success'} btn-full`}
          onClick={handleToggle}
          disabled={loading}
        >
          {loading ? 'Processing...' : (isActive ? 'Stop' : 'Start')}
        </button>
      </div>
    </div>
  );
};

export default ConnectionCard;
