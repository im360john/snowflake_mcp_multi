# Snowflake MCP Multi-Connection Server

A web-based management system for creating and managing multiple Snowflake database connections, each with its own MCP (Model Context Protocol) server and unique SSE endpoint.

## Features

- 🏔️ **Multi-Connection Management**: Create and manage multiple Snowflake connections
- 🔌 **Unique SSE Endpoints**: Each connection gets its own isolated MCP server with a unique endpoint
- 🎨 **Modern Web UI**: React-based interface for easy connection management
- 🔄 **Dynamic Server Management**: Start/stop connections on demand
- 💾 **Persistent Storage**: Connection configurations saved in PostgreSQL
- 🚀 **Cloud Ready**: Designed for deployment on Render.com or similar platforms

## Quick Start

### Deploy on Render

[![Deploy to Render](https://render.com/images/deploy-to-render-button.svg)](https://render.com/deploy?repo=https://github.com/YOUR_USERNAME/snowflake-mcp-multi)

### Local Development

1. Clone the repository:
```bash
git clone https://github.com/YOUR_USERNAME/snowflake-mcp-multi.git
cd snowflake-mcp-multi
```

2. Set up environment variables:
```bash
cp backend/.env.example backend/.env
cp frontend/.env.example frontend/.env
```

3. Run with Docker Compose:
```bash
docker-compose up
```

4. Access the application:
- Frontend: http://localhost:3000
- Backend API: http://localhost:8000
- MCP Servers: http://localhost:8100-8200/sse

## Configuration

### Backend Environment Variables

- `DATABASE_URL`: PostgreSQL connection string
- `SECRET_KEY`: Secret key for security
- `PORT_RANGE_START`: Starting port for MCP servers (default: 8100)
- `PORT_RANGE_END`: Ending port for MCP servers (default: 8200)

### Frontend Environment Variables

- `REACT_APP_API_URL`: Backend API URL

## Usage

1. **Create a Connection**: Click "Add New Connection" and fill in your Snowflake credentials
2. **Start the Connection**: Click "Start" to activate the MCP server
3. **Copy SSE Endpoint**: Use the provided endpoint in your MCP client (e.g., Claude Desktop)
4. **Manage Connections**: Start, stop, or delete connections as needed

## API Documentation

Once running, access the interactive API documentation at:
- Swagger UI: http://localhost:8000/docs
- ReDoc: http://localhost:8000/redoc

## Security Considerations

⚠️ **Important**: This is a development version. For production use:

- Implement proper password encryption
- Add authentication to the management API
- Use HTTPS/TLS for all communications
- Implement connection-level API keys
- Add rate limiting and access controls
- Use a secrets management service

## Architecture

- **Frontend**: React with TypeScript
- **Backend**: FastAPI with FastMCP
- **Database**: PostgreSQL
- **MCP Servers**: Individual processes with SSE transport
- **Container**: Docker with Docker Compose

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- Built with [FastMCP](https://github.com/jlowin/fastmcp)
- Inspired by [MCP Snowflake Server](https://github.com/isaacwasserman/mcp-snowflake-server)
- Designed for use with [Claude Desktop](https://claude.ai) and other MCP clients
