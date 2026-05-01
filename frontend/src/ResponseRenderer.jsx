import React from "react";
import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";

// 🔍 Detect JSON safely
const isJSON = (text) => {
  try {
    const trimmed = text.trim();
    // Ensure it looks like JSON before parsing
    if (trimmed.startsWith("{") || trimmed.startsWith("[")) {
      const parsed = JSON.parse(trimmed);
      return typeof parsed === "object";
    }
    return false;
  } catch {
    return false;
  }
};

// 📊 Render Raw JSON nicely (if the API sends a pure JSON string)
const renderJSON = (data) => {
  if (Array.isArray(data) && data.length > 0) {
    const keys = Object.keys(data[0] || {});
    return (
      <div className="table-container">
        <table className="markdown-renderer-table">
          <thead>
            <tr>
              {keys.map((k) => (
                <th key={k}>{k}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {data.map((row, i) => (
              <tr key={i}>
                {keys.map((k) => (
                  <td key={k}>{String(row[k])}</td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    );
  }

  return (
    <pre className="json-card">
      {JSON.stringify(data, null, 2)}
    </pre>
  );
};

const ResponseRenderer = ({ text }) => {
  if (!text) return null;

  // Case 1: Detect and Render Raw JSON
  if (isJSON(text)) {
    const parsed = JSON.parse(text);
    return renderJSON(parsed);
  }

  // Case 2: Render Markdown (Standard text, SQL blocks, and Markdown Tables)
  // We use remarkGfm to support the | pipe | table syntax
  return (
    <div className="markdown-content">
      <ReactMarkdown remarkPlugins={[remarkGfm]}>
        {text}
      </ReactMarkdown>
    </div>
  );
};

export default ResponseRenderer;