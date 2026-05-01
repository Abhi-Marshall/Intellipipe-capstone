// const express = require("express");
// const axios = require("axios");
// const cors = require("cors")
// require("dotenv").config();

// const app = express();
// app.use(cors());
// app.use(express.json());

// app.post("/full-refresh", async (req, res) => {
//   try {
//     console.log("Incoming Body:", req.body);

//     const { question } = req.body;

//     const response = await axios.post(
//       "https://dbc-5e2e6f3e-0d80.cloud.databricks.com/serving-endpoints/supervisor_mcp/invocations",
//       {
//         dataframe_split: {
//           columns: ["messages"],
//           data: [[JSON.stringify([{ role: "user", content: question }])]],
//         },
//       },
//       {
//         headers: {
//           Authorization: `Bearer ${process.env.DATABRICKS_TOKEN}`,
//           "Content-Type": "application/json", 
//         },
//       }
//     );

//     const content =
//       response.data.predictions.choices[0].message.content;

//     console.log("Content:",content)

//     res.json({
//       answer: content,
//       runId: Date.now(),
//     });

//   } catch (err) {
//     console.error("FULL REFRESH ERROR:");
//     console.error(err.response?.data || err.message || err);

//     res.status(500).json({
//       error: "Failed to trigger pipeline",
//       details: err.response?.data || err.message,
//     });
//   }
// });

// app.get("/pipeline-status", async (req, res) => {
//   try {
//     const { pipelineId } = req.query;

//     console.log("Pipeline Id",pipelineId)

//     const response = await axios.post(
//       "https://dbc-5e2e6f3e-0d80.cloud.databricks.com/serving-endpoints/supervisor_mcp/invocations",
//       {
//         dataframe_split: {
//           columns: ["messages"],
//           data: [[JSON.stringify([{
//             role: "user",
//             content: `Check pipeline health for pipeline id: ${pipelineId}`
//           }])]],
//         },
//       },
//       {
//         headers: {
//           Authorization: `Bearer ${process.env.DATABRICKS_TOKEN}`,
//         },
//       }
//     );

//     const content =
//       response.data.predictions.choices[0].message.content;

//     let status = "RUNNING";

//     if (content.includes("healthy")) status = "COMPLETED";
//     else if (content.includes("FAILED")) status = "FAILED";

//     res.json({
//       status,
//       details: content,
//     });

//   } catch (err) {
//     res.status(500).json({ error: "Status check failed" });
//   }
// });

// app.post("/run-pipeline", async (req, res) => {
//   try {
//     const { question } = req.body;
//     console.log("Question",question)

//     const response = await axios.post(
//       "https://dbc-5e2e6f3e-0d80.cloud.databricks.com/serving-endpoints/supervisor_mcp/invocations",
//       {
//         dataframe_split: {
//           columns: ["messages"],
//           data: [
//             [
//               JSON.stringify([
//                 {
//                   role: "user",
//                   content: question
//                 }
//               ])
//             ]
//           ]
//         }
//       },
//       {
//         headers: {
//           Authorization: `Bearer ${process.env.DATABRICKS_TOKEN}`,
//           "Content-Type": "application/json"
//         }
//       }
//     );

//     const content =
//       response.data.predictions.choices[0].message.content;

//     console.log("response:",content)
//     res.json({ answer: content });

//   } catch (error) {
//     console.error(error.response?.data || error.message);
//     res.status(500).json({ error: "Something went wrong" });
//   }
// });

// app.post("/ask", async (req, res) => {
//   try {
//     const { question } = req.body;
//     console.log("Question",question)

//     const response = await axios.post(
//       "https://dbc-5e2e6f3e-0d80.cloud.databricks.com/serving-endpoints/supervisor_mcp/invocations",
//       {
//         dataframe_split: {
//           columns: ["messages"],
//           data: [
//             [
//               JSON.stringify([
//                 {
//                   role: "user",
//                   content: question
//                 }
//               ])
//             ]
//           ]
//         }
//       },
//       {
//         headers: {
//           Authorization: `Bearer ${process.env.DATABRICKS_TOKEN}`,
//           "Content-Type": "application/json"
//         }
//       }
//     );

//     const content =
//       response.data.predictions.choices[0].message.content;

//     console.log("response:",content)
//     res.json({ answer: content });

//   } catch (error) {
//     console.error(error.response?.data || error.message);
//     res.status(500).json({ error: "Something went wrong" });
//   }
// });


// app.get("/pipeline-health", async (req, res) => {
//   try {
//     const pipelineId="b5b42690-b884-4945-8da6-74b19aa93553"

//     const response = await axios.get(
//       `${process.env.DATABRICKS_HOST}/api/2.0/pipelines/${pipelineId}`,
//       {
//         headers: {
//           Authorization: `Bearer ${process.env.DATABRICKS_TOKEN}`,
//         },
//       }
//     );

//     const pipeline = response.data;

//     // 🔍 Extract health info
//     const state = pipeline?.state; 
//     const health = pipeline?.health;

//     let status = state || "UNKNOWN";

//     res.json({
//       pipeline_id: pipelineId,
//       status,
//       health,
//       raw: pipeline,
//     });

//   } catch (error) {
//     console.log("Error:", error.response?.data || error.message);

//     res.status(500).json({
//       error: "Failed to fetch pipeline health",
//       details: error.response?.data || error.message,
//     });
//   }
// });

// app.listen(5000, () => console.log("Server running on port 5000"));




const express = require("express");
const axios = require("axios");
const cors = require("cors");
require("dotenv").config();

const app = express();
app.use(cors());
app.use(express.json());

const FIXED_ID = "b5b42690-b884-4945-8da6-74b19aa93553";
const DATABRICKS_URL = "https://dbc-5e2e6f3e-0d80.cloud.databricks.com/serving-endpoints/supervisor_mcp_agent/invocations";

// Reusable Databricks Supervisor Caller
async function callSupervisor(content) {
  const response = await axios.post(
    DATABRICKS_URL,
    {
      dataframe_split: {
        columns: ["messages"],
        data: [[JSON.stringify([{ role: "user", content: content }])]],
      },
    },
    {
      headers: {
        Authorization: `Bearer ${process.env.DATABRICKS_TOKEN}`,
        "Content-Type": "application/json",
      },
    }
  );
  return response.data.predictions.choices[0].message.content;
}

app.post("/full-refresh", async (req, res) => {
  try {
    const content = await callSupervisor("Trigger full refresh for pipeline " + FIXED_ID);
    res.json({ answer: content });
  } catch (err) {
    res.status(500).json({ error: "Trigger failed" });
  }
});

app.post("/run-pipeline", async (req, res) => {
  try {
    const content = await callSupervisor("Trigger incremental run for pipeline " + FIXED_ID);
    res.json({ answer: content });
  } catch (err) {
    res.status(500).json({ error: "Trigger failed" });
  }
});

app.get("/pipeline-status", async (req, res) => {
  try {
    // Specifically ask the agent if the CURRENT update is finished
    const content = await callSupervisor(
      `Is the pipeline ${FIXED_ID} currently 'RUNNING' its update, or has the latest update 'COMPLETED'? Ensure gold layer tables are finished.`
    );
    
    let status = "RUNNING";
    const lowerContent = content.toLowerCase();

    // Logic: Only stop if we see success keywords AND no mention of ongoing processing
    const hasFinished = lowerContent.includes("completed") || lowerContent.includes("healthy") || lowerContent.includes("finished");
    const isStillActive = lowerContent.includes("running") || lowerContent.includes("processing") || lowerContent.includes("refreshing");

    if (hasFinished && !isStillActive) {
      status = "COMPLETED";
    } else if (lowerContent.includes("failed") || lowerContent.includes("error")) {
      status = "FAILED";
    }

    res.json({ status, details: content });
  } catch (err) {
    res.status(500).json({ error: "Health check failed" });
  }
});

app.post("/ask", async (req, res) => {
  try {
    const { question } = req.body;
    console.log("Question:",question)
    const content = await callSupervisor(question);
    console.log("Content:",content)
    res.json({ answer: content });
  } catch (err) {
    res.status(500).json({ error: "Query failed" });
  }
});

app.listen(5000, () => console.log("Server active on port 5000"));