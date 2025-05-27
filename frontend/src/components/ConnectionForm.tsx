import React, { useState } from 'react';

interface ConnectionFormProps {
  onSubmit: (data: any) => void;
  onCancel: () => void;
}

const ConnectionForm: React.FC<ConnectionFormProps> = ({ onSubmit, onCancel }) => {
  const [formData, setFormData] = useState({
    name: '',
    account: '',
    user: '',
    password: '',
    warehouse: '',
    database: '',
    schema: '',
    role: ''
  });

  const handleChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    setFormData({
      ...formData,
      [e.target.name]: e.target.value
    });
  };

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    onSubmit(formData);
  };

  return (
    <div className="connection-form-container">
      <form className="connection-form" onSubmit={handleSubmit}>
        <h2>Create New Snowflake Connection</h2>
        
        <div className="form-grid">
          <div className="form-group">
            <label htmlFor="name">Connection Name</label>
            <input
              type="text"
              id="name"
              name="name"
              value={formData.name}
              onChange={handleChange}
              required
              placeholder="My Snowflake DB"
            />
          </div>

          <div className="form-group">
            <label htmlFor="account">Account</label>
            <input
              type="text"
              id="account"
              name="account"
              value={formData.account}
              onChange={handleChange}
              required
              placeholder="abc12345.us-east-1"
            />
          </div>

          <div className="form-group">
            <label htmlFor="user">Username</label>
            <input
              type="text"
              id="user"
              name="user"
              value={formData.user}
              onChange={handleChange}
              required
              placeholder="snowflake_user"
            />
          </div>

          <div className="form-group">
            <label htmlFor="password">Password</label>
            <input
              type="password"
              id="password"
              name="password"
              value={formData.password}
              onChange={handleChange}
              required
              placeholder="••••••••"
            />
          </div>

          <div className="form-group">
            <label htmlFor="warehouse">Warehouse</label>
            <input
              type="text"
              id="warehouse"
              name="warehouse"
              value={formData.warehouse}
              onChange={handleChange}
              required
              placeholder="COMPUTE_WH"
            />
          </div>

          <div className="form-group">
            <label htmlFor="database">Database</label>
            <input
              type="text"
              id="database"
              name="database"
              value={formData.database}
              onChange={handleChange}
              required
              placeholder="MY_DATABASE"
            />
          </div>

          <div className="form-group">
            <label htmlFor="schema">Schema</label>
            <input
              type="text"
              id="schema"
              name="schema"
              value={formData.schema}
              onChange={handleChange}
              required
              placeholder="PUBLIC"
            />
          </div>

          <div className="form-group">
            <label htmlFor="role">Role</label>
            <input
              type="text"
              id="role"
              name="role"
              value={formData.role}
              onChange={handleChange}
              required
              placeholder="ACCOUNTADMIN"
            />
          </div>
        </div>

        <div className="form-actions">
          <button type="button" className="btn btn-secondary" onClick={onCancel}>
            Cancel
          </button>
          <button type="submit" className="btn btn-primary">
            Create Connection
          </button>
        </div>
      </form>
    </div>
  );
};

export default ConnectionForm;
