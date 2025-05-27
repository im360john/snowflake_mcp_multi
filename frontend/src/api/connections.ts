const API_BASE_URL = process.env.REACT_APP_API_URL || 'http://localhost:8000';

export interface Connection {
  id: string;
  name: string;
  account: string;
  sse_endpoint: string;
  active: boolean;
  created_at: string;
}

export interface ConnectionCreate {
  name: string;
  account: string;
  user: string;
  password: string;
  warehouse: string;
  database: string;
  schema: string;
  role: string;
}

export async function listConnections(): Promise<Connection[]> {
  const response = await fetch(`${API_BASE_URL}/connections`);
  if (!response.ok) throw new Error('Failed to fetch connections');
  return response.json();
}

export async function createConnection(data: ConnectionCreate): Promise<Connection> {
  const response = await fetch(`${API_BASE_URL}/connections`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(data),
  });
  if (!response.ok) throw new Error('Failed to create connection');
  return response.json();
}

export async function deleteConnection(id: string): Promise<void> {
  const response = await fetch(`${API_BASE_URL}/connections/${id}`, {
    method: 'DELETE',
  });
  if (!response.ok) throw new Error('Failed to delete connection');
}

export async function startConnection(id: string): Promise<{ message: string; sse_endpoint: string }> {
  const response = await fetch(`${API_BASE_URL}/connections/${id}/start`, {
    method: 'POST',
  });
  if (!response.ok) throw new Error('Failed to start connection');
  return response.json();
}

export async function stopConnection(id: string): Promise<{ message: string }> {
  const response = await fetch(`${API_BASE_URL}/connections/${id}/stop`, {
    method: 'POST',
  });
  if (!response.ok) throw new Error('Failed to stop connection');
  return response.json();
}
