/**
 * MCP Blog Generation Endpoint
 * 
 * POST /api/mcp/generate
 * 
 * Bridges the frontend UI with the MCP server to generate blog posts using Claude.
 */

import { MCPBlogServer } from "@mcp/index";
import { blogTools } from "@mcp/tools/index";
import { NextRequest, NextResponse } from "next/server";

// Type definitions for MCP communication
interface OneApiRequest {
  headers?: Record<string, string>;
}

interface OneApiResponse {
  headers?: Record<string, string>;
}

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
const claudeApiKey = process.env.ANTHROPIC_API_KEY;

let mcpServer: MCPBlogServer | null = null;

function getMCPServer(): MCPBlogServer {
  if (!mcpServer) {
    mcpServer = new MCPBlogServer({ apiKey: mcpApiKey });
    // Register all tools
    blogTools.forEach((tool) => mcpServer!.registerTool(tool));
  }
  return mcpServer;
}

export async function POST(request: NextRequest) {
  try {
    // Check if Claude API key is configured
    if (!claudeApiKey) {
      return NextResponse.json(
        {
          success: false,
          error: "Claude API not configured. Set ANTHROPIC_API_KEY environment variable.",
        },
        { status: 500 }
      );
    }

    // Parse request body
    const body = await request.json();
    const { topic, style, targetLength, keywords } = body;

    // Validate required parameters
    if (!topic || !style) {
      return NextResponse.json(
        {
          success: false,
          error: "Missing required parameters: topic and style are required",
        },
        { status: 400 }
      );
    }

    // Validate style enum
    const validStyles = ["formal", "casual", "technical", "conversational"];
    if (!validStyles.includes(style)) {
      return NextResponse.json(
        {
          success: false,
          error: `Invalid style. Must be one of: ${validStyles.join(", ")}`,
        },
        { status: 400 }
      );
    }

    console.log(
      `[MCP API] 🚀 Generating blog post: topic="${topic}", style="${style}"`
    );

    // Create MCP request with auth headers
    const mcpRequest: OneApiRequest = {
      headers: {
        "x-mcp-key": mcpApiKey,
      },
    };

    // Get MCP server instance
    const server = getMCPServer();

    // Execute the generate_blog_post tool via MCP
    const response = (await server.handleRequest(
      mcpRequest as any,
      {
        tool: "generate_blog_post",
        params: {
          topic,
          style,
          targetLength: targetLength || 1500,
          keywords: keywords || "",
        },
      } as any,
      {} as any
    )) as MCPResponse;

    if (!response.success) {
      console.error("[MCP API] ❌ Generation failed:", response.error);
      return NextResponse.json(
        {
          success: false,
          error: response.error,
        },
        { status: 500 }
      );
    }

    console.log("[MCP API] ✅ Blog post generated successfully");

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
 * GET /api/mcp/generate
 * Returns capabilities and available generation options
 */
export async function GET() {
  const server = getMCPServer();
  const capabilities = server.getCapabilities();

  return NextResponse.json({
    success: true,
    message: "MCP Blog Generation API",
    capabilities,
    styles: ["formal", "casual", "technical", "conversational"],
    endpoint: {
      method: "POST",
      path: "/api/mcp/generate",
      parameters: {
        topic: { type: "string", required: true },
        style: {
          type: "string",
          enum: ["formal", "casual", "technical", "conversational"],
          required: true,
        },
        targetLength: { type: "number", required: false, default: 1500 },
        keywords: { type: "string", required: false },
      },
    },
  });
}
