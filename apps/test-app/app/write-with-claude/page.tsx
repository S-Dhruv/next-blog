"use client";

import { useState } from "react";
import { WriteWithClaude } from "@/components/WriteWithClaude";
import styles from "./page.module.css";

interface GeneratedContent {
  content: string;
  metadata: {
    topic: string;
    style: string;
    generatedAt: string;
  };
}

export default function WriteWithClaudePage() {
  const [generatedContent, setGeneratedContent] =
    useState<GeneratedContent | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [isPublishing, setIsPublishing] = useState(false);

  const handleContentGenerated = (content: string, metadata: any) => {
    console.log(
      "[WriteWithClaudePage] 📝 Content generated, displaying draft..."
    );
    setGeneratedContent({ content, metadata });
    setError(null);
  };

  const handleError = (errorMessage: string) => {
    console.error("[WriteWithClaudePage] 💥 Error:", errorMessage);
    setError(errorMessage);
    setGeneratedContent(null);
  };

  const handleCopy = () => {
    if (generatedContent) {
      navigator.clipboard.writeText(generatedContent.content);
      alert("✅ Content copied to clipboard!");
    }
  };

  const handleEditAndPublish = () => {
    if (!generatedContent) return;

    setIsPublishing(true);
    console.log("[WriteWithClaudePage] 📤 Publishing blog...");

    try {
      // Extract metadata for the dashboard
      const titleMatch = generatedContent.content.match(/^# (.+?)$/m);
      const title = titleMatch ? titleMatch[1] : generatedContent.metadata.topic;

      const excerptMatch = generatedContent.content.match(/^(?:#{1,6} .+\n)+(.+?)$/m);
      const excerpt = excerptMatch
        ? excerptMatch[1].substring(0, 200)
        : generatedContent.metadata.topic;

      const slug = title
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, "-")
        .replace(/^-|-$/g, "");

      // Call MCP publish_blog tool directly
      publishToMCP({
        title,
        slug,
        excerpt,
        content: generatedContent.content,
        status: "draft",
      });
    } catch (err) {
      const errorMessage =
        err instanceof Error ? err.message : "Failed to publish";
      console.error("[WriteWithClaudePage] ❌ Error:", errorMessage);
      alert(`Error: ${errorMessage}`);
      setIsPublishing(false);
    }
  };

  const handleQuickPublish = async () => {
    if (!generatedContent) return;

    setIsPublishing(true);
    console.log("[WriteWithClaudePage] 🚀 Quick publishing...");

    try {
      // Extract title from content (first heading)
      const titleMatch = generatedContent.content.match(/^# (.+?)$/m);
      const title = titleMatch ? titleMatch[1] : generatedContent.metadata.topic;

      // Create excerpt from first paragraph
      const excerptMatch = generatedContent.content.match(
        /^(?:#{1,6} .+\n)+(.+?)$/m
      );
      const excerpt = excerptMatch
        ? excerptMatch[1].substring(0, 150)
        : generatedContent.metadata.topic;

      // Create slug from title
      const slug = title
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, "-")
        .replace(/^-|-$/g, "");

      // Call MCP publish_blog tool directly
      await publishToMCP({
        title,
        slug,
        excerpt,
        content: generatedContent.content,
        status: "published",
      });
    } catch (err) {
      const errorMessage =
        err instanceof Error ? err.message : "Failed to publish";
      console.error("[WriteWithClaudePage] ❌ Error:", errorMessage);
      alert(`Error: ${errorMessage}`);
    } finally {
      setIsPublishing(false);
    }
  };

  const publishToMCP = async (blogData: any) => {
    try {
      const response = await fetch("/api/mcp/publish", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          tool: "publish_blog",
          params: blogData,
        }),
      });

      const result = await response.json();

      if (!response.ok || !result.success) {
        const errorMessage = result.error || "Failed to publish blog";
        alert(`Error: ${errorMessage}`);
        console.error("[WriteWithClaudePage] ❌ Publish failed:", errorMessage);
        return;
      }

      console.log("[WriteWithClaudePage] ✅ Blog published successfully");
      alert(
        `✅ Blog "${blogData.title}" published successfully! Blog ID: ${result.data.blogId}`
      );

      // Reset state
      setGeneratedContent(null);
      setError(null);

      // Optionally navigate to blog or refresh
      setTimeout(() => {
        window.location.reload();
      }, 2000);
    } catch (err) {
      const errorMessage =
        err instanceof Error ? err.message : "Failed to publish";
      console.error("[WriteWithClaudePage] 💥 Publish error:", err);
      alert(`Error: ${errorMessage}`);
    } finally {
      setIsPublishing(false);
    }
  };

  return (
    <div className={styles.page}>
      <header className={styles.header}>
        <h1 className={styles.title}>AI Blog Writer</h1>
        <p className={styles.subtitle}>
          Write professional blog posts in seconds with Claude AI
        </p>
      </header>

      <main className={styles.main}>
        <div className={styles.container}>
          <div className={styles.grid}>
            {/* Input Column */}
            <div className={styles.inputColumn}>
              <WriteWithClaude
                onContentGenerated={handleContentGenerated}
                onError={handleError}
              />
            </div>

            {/* Output Column */}
            <div className={styles.outputColumn}>
              {generatedContent ? (
                <div className={styles.draftCard}>
                  <div className={styles.draftHeader}>
                    <h2 className={styles.draftTitle}>📄 Generated Draft</h2>
                    <button
                      onClick={handleCopy}
                      className={styles.copyButton}
                      title="Copy content to clipboard"
                    >
                      📋 Copy
                    </button>
                  </div>

                  <div className={styles.draftMetadata}>
                    <span className={styles.metadataItem}>
                      <strong>Topic:</strong> {generatedContent.metadata.topic}
                    </span>
                    <span className={styles.metadataItem}>
                      <strong>Style:</strong>{" "}
                      {generatedContent.metadata.style}
                    </span>
                    <span className={styles.metadataItem}>
                      <strong>Generated:</strong>{" "}
                      {new Date(
                        generatedContent.metadata.generatedAt
                      ).toLocaleString()}
                    </span>
                  </div>

                  <div className={styles.draftContent}>
                    {generatedContent.content
                      .split("\n")
                      .map((paragraph, index) => {
                        if (paragraph.startsWith("#")) {
                          const level = paragraph.match(/^#+/)?.[0].length ?? 1;
                          const text = paragraph.replace(/^#+\s*/, "");
                          return (
                            <div
                              key={index}
                              className={styles[`heading${level}`]}
                            >
                              {text}
                            </div>
                          );
                        }
                        if (paragraph.trim()) {
                          return (
                            <p key={index} className={styles.paragraph}>
                              {paragraph}
                            </p>
                          );
                        }
                        return null;
                      })}
                  </div>

                  <div className={styles.draftFooter}>
                    <p className={styles.footerText}>
                      ✅ Ready to edit and publish! Review the content and make
                      any adjustments before publishing.
                    </p>
                    <div className={styles.actionButtons}>
                      <button
                        onClick={handleEditAndPublish}
                        disabled={isPublishing}
                        className={styles.editButton}
                      >
                        {isPublishing ? (
                          <>
                            <span className={styles.spinner} />
                            Publishing...
                          </>
                        ) : (
                          <>✏️ Edit & Publish</>
                        )}
                      </button>
                      <button
                        onClick={handleQuickPublish}
                        disabled={isPublishing}
                        className={styles.publishButton}
                      >
                        {isPublishing ? (
                          <>
                            <span className={styles.spinner} />
                            Publishing...
                          </>
                        ) : (
                          <>🚀 Publish as Draft</>
                        )}
                      </button>
                    </div>
                  </div>
                </div>
              ) : error ? (
                <div className={styles.errorCard}>
                  <h3 className={styles.errorTitle}>⚠️ Error</h3>
                  <p className={styles.errorMessage}>{error}</p>
                </div>
              ) : (
                <div className={styles.placeholderCard}>
                  <div className={styles.placeholderIcon}>✍️</div>
                  <h3 className={styles.placeholderTitle}>
                    Your Generated Draft Will Appear Here
                  </h3>
                  <p className={styles.placeholderText}>
                    Fill out the form on the left and click "Generate Blog
                    Post" to create your AI-powered draft using Claude.
                  </p>
                </div>
              )}
            </div>
          </div>
        </div>
      </main>

      <footer className={styles.footer}>
        <div className={styles.footerContent}>
          <p>
            🚀 Powered by Claude AI • MCP Server Integration • Next.js + TypeScript
          </p>
        </div>
      </footer>
    </div>
  );
}
