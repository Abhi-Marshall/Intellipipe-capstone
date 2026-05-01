// // import express from "express";
// // import axios from "axios";
// // import cors from "cors";
// // import dotenv from "dotenv";

// const express = require("express")
// const axios = require("axios")
// const cors = require("cors")
// const dotenv = require("dotenv")

// dotenv.config();

// const app = express();
// app.use(cors());
// app.use(express.json());

// app.get("/",(req,res)=>{
//     res.send("Hii Backlend")
// })

// app.post("/query", async (req, res) => {
//   try {
//     const userQuery = req.body.query;

//     const response = await axios.post(
//       process.env.DATABRICKS_URL,
//       {
//         inputs: userQuery
//       },
//       {
//         headers: {
//           "Authorization": `Bearer ${process.env.DATABRICKS_TOKEN}`,
//           "Content-Type": "application/json"
//         }
//       }
//     );

//     res.json(response.data);

//   } catch (error) {
//     console.error(error.response?.data || error.message);
//     res.status(500).json({ error: "Something went wrong" });
//   }
// });

// app.listen(process.env.PORT, () => {
//   console.log(`Server running on port ${process.env.PORT}`);
// });


const axios = require("axios");
require('dotenv').config();

async function callEndpoint() {
  try {
    const response = await axios.post(
  "https://dbc-5e2e6f3e-0d80.cloud.databricks.com/serving-endpoints/supervisor_mcp/invocations",
  {
    dataframe_split: {
      columns: ["messages"],
      data: [
        [
          JSON.stringify([
            {
              role: "user",
              content: "Show me metrics for the last 24 hours"
            }
          ])
        ]
      ]
    }
  },
  {
    headers: {
      Authorization: `Bearer ${process.env.DATABRICKS_TOKEN}`,
      "Content-Type": "application/json"
    }
  }
);

    console.log("Success:",JSON.stringify(response.data, null, 2));

  } catch (error) {
    console.error("ERROR:", JSON.stringify(error.response?.data, null, 2));
  }
}

callEndpoint();