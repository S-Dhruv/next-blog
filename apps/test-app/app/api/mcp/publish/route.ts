/**
 * MCP Tool Execution Endpoint
 * 
 * POST /api/mcp/publish
 * 
 * Executes MCP tools (publish_blog, get_blog, list_blogs, update_blog, delete_blog)
 */

import { MCPBlogServer } from "@mcp/index";
import { blogTools } from "@mcp/tools/index";
import { NextRequest, NextResponse } from "next/server";

// Type definitions for MCP communication
interface MCPRequest {
  tool: string;
  params: Record<string, any>;
}

interface MCPResponse {
  success: boolean;
  data?: any;
  error?: string;
}

// Initialize MCP server (with API key from environment)
const mcpApiKey = process.env.MCP_API_KEY || "default-mcp-key-12345";

let mcpServer: MCPBlogServer | null = null;

function getMCPServer(): MCPBlogServer {
  if (!mcpServer) {
    mcpServer = new MCPBlogServer({ apiKey: mcpApiKey });
    // Register all tools (both content generation and CRUD)
    blogTools.forEach((tool) => mcpServer!.registerTool(tool));
  }
  return mcpServer;
}

export async function POST(request: NextRequest) {
  try {
    // Parse request body
    const body = await request.json();
    const mcpRequest: MCPRequest = body;

    // Validate request structure
    if (!mcpRequest.tool || !mcpRequest.params) {
      return NextResponse.json(
        {
          success: false,
          error: "Invalid request format. Expected: { tool: string, params: object }",
        },
        { status: 400 }
      );
    }

    console.log(`[MCP API] 📤 Executing tool: ${mcpRequest.tool}`);

    // Convert Next.js headers to OneApiRequest format
    const oneApiRequest = {
      headers: {
        get: (name: string) => request.headers.get(name),
      } as any,
    };

    // Execute on MCP server
    const server = getMCPServer();
    const response = (await server.handleRequest(
      oneApiRequest,
      mcpRequest
    )) as MCPResponse;

    if (!response.success) {
      console.error("[MCP API] ❌ Tool execution failed:", response.error);
      return NextResponse.json(
        {
          success: false,
          error: response.error,
        },
        { status: 400 }
      );
    }

    console.log("[MCP API] ✅ Tool executed successfully");

    return NextResponse.json({
      success: true,
      data: response.data,
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.error("[MCP API] 💥 Unexpected error:", message, error);
    return NextResponse.json(
      {
        success: false,
        error: message,
      },
      { status: 500 }
    );
  }
}

/**
 * GET /api/mcp/publish
 * Returns available MCP tools
 */
export async function GET() {
  const server = getMCPServer();
  const capabilities = server.getCapabilities();

  return NextResponse.json({
    success: true,
    message: "MCP Blog Tools API",
    capabilities,
  });
}
