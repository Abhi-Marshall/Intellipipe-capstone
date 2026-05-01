// import { useState, useEffect, useRef } from "react";
// import ResponseRenderer from "./ResponseRenderer";
// import "./App.css";

// function App() {
//   const [question, setQuestion] = useState("");
//   const [messages, setMessages] = useState([]);
//   const [loading, setLoading] = useState(false);
//   const [activeTab, setActiveTab] = useState("home");

//   const chatEndRef = useRef(null);
//   const pollingRef = useRef(null); // 🔥 store interval

//   // 🔽 Auto scroll
//   useEffect(() => {
//     chatEndRef.current?.scrollIntoView({ behavior: "smooth" });
//   }, [messages]);

//   // 🧹 Cleanup polling when component unmounts
//   useEffect(() => {
//     return () => {
//       if (pollingRef.current) clearInterval(pollingRef.current);
//     };
//   }, []);

//   // 🔁 Common API caller (ONLY for chat)
//   const callAPI = async (endpoint, payload) => {
//     setLoading(true);

//     try {
//       const res = await fetch(`http://localhost:5000/${endpoint}`, {
//         method: "POST",
//         headers: {
//           "Content-Type": "application/json",
//         },
//         body: JSON.stringify(payload),
//       });

//       const data = await res.json();

//       const botMessage = {
//         type: "bot",
//         text: data?.answer || "No response received",
//       };

//       setMessages((prev) => [...prev, botMessage]);
//     } catch (error) {
//       setMessages((prev) => [
//         ...prev,
//         { type: "bot", text: "⚠️ Error connecting to server" },
//       ]);
//     }

//     setLoading(false);
//   };

//   // 💬 Chat send
//   const handleSend = async () => {
//     if (!question.trim()) return;

//     const userMessage = { type: "user", text: question };
//     setMessages((prev) => [...prev, userMessage]);

//     const q = question;
//     setQuestion("");

//     await callAPI("ask", { question: q });
//   };

//   // 🔁 START POLLING FUNCTION
//   const startPolling = (runId) => {
//     // clear old polling if exists
//     if (pollingRef.current) clearInterval(pollingRef.current);

//     pollingRef.current = setInterval(async () => {
//       try {
//         const res = await fetch(
//           `http://localhost:5000/pipeline-status?pipelineId=b5b42690-b884-4945-8da6-74b19aa93553`
//         );

        
//         const data = await res.json();
//         console.log("Response from pipeline health:",data)

//         setMessages((prev) => [
//           ...prev,
//           {
//             type: "bot",
//             text: `📡 Pipeline Status: ${data.status}`,
//           },
//         ]);

//         // ✅ STOP CONDITION
//         if (data.status === "COMPLETED" || data.status === "FAILED") {
//           clearInterval(pollingRef.current);

//           setMessages((prev) => [
//             ...prev,
//             {
//               type: "bot",
//               text: `✅ FINAL STATUS: ${data.status}\n\n${data.details}`,
//             },
//           ]);
//         }
//       } catch (err) {
//         console.error(err);
//         clearInterval(pollingRef.current);

//         setMessages((prev) => [
//           ...prev,
//           {
//             type: "bot",
//             text: "⚠️ Failed to fetch pipeline status",
//           },
//         ]);
//       }
//     }, 60000); // ⏱️ every 60 sec
//   };

//   // 🔄 FULL REFRESH
//   const handleFullRefresh = async () => {
//     setActiveTab("chat");

//     setMessages((prev) => [
//       ...prev,
//       { type: "bot", text: "🚀 Triggering FULL pipeline refresh..." },
//     ]);

//     try {
//       const res = await fetch("http://localhost:5000/full-refresh", {
//         method: "POST",
//         headers: {
//           "Content-Type": "application/json",
//         },
//         body: JSON.stringify({ question: "Refresh the Pipeline" }),
//       });

//       const data = await res.json();

//       setMessages((prev) => [
//         ...prev,
//         { type: "bot", text: data.answer || "Pipeline triggered" },
//         { type: "bot", text: "⏳ Monitoring pipeline..." },
//       ]);

//       const runId = data.runId || Date.now(); // fallback
//       startPolling(runId);

//     } catch (error) {
//       setMessages((prev) => [
//         ...prev,
//         { type: "bot", text: "⚠️ Failed to trigger pipeline" },
//       ]);
//     }
//   };

//   // ⚡ PARTIAL RUN
//   const handlePartialRefresh = async () => {
//     setActiveTab("chat");

//     setMessages((prev) => [
//       ...prev,
//       { type: "bot", text: "⚡ Running pipeline..." },
//     ]);

//     try {
//       const res = await fetch("http://localhost:5000/run-pipeline", {
//         method: "POST",
//         headers: {
//           "Content-Type": "application/json",
//         },
//         body: JSON.stringify({ question: "Run the pipeline" }),
//       });

//       const data = await res.json();

//       setMessages((prev) => [
//         ...prev,
//         { type: "bot", text: data.answer || "Pipeline triggered" },
//         { type: "bot", text: "⏳ Monitoring pipeline..." },
//       ]);

//       const runId = data.runId || Date.now();
//       startPolling(runId);

//     } catch (error) {
//       setMessages((prev) => [
//         ...prev,
//         { type: "bot", text: "⚠️ Failed to trigger pipeline" },
//       ]);
//     }
//   };

//   return (
//   <div className="app-wrapper">
//     {/* 🔷 HEADER */}
//     <div className="header">
//       {/* <h2 className="company">Sigmoid</h2> */}
//       <h1 className="system">IntelliPipe Control System</h1>
//       <p className="subtitle">
//         Intelligent Data Pipeline Monitoring & Control Dashboard
//       </p>
//     </div>

//     <div className="container">
//       {/* 🔘 ACTION BUTTONS */}
//       <div className="top-buttons">
//         <button onClick={handleFullRefresh} className="btn">
//           🔄 Refresh Pipeline
//         </button>

//         <button onClick={handlePartialRefresh} className="btn">
//           ⚡ Run Pipeline
//         </button>

//         <button onClick={() => setActiveTab("chat")} className="btn">
//           💬 Chat
//         </button>

//         <button onClick={() => setActiveTab("home")} className="btn">
//           🏠 Home
//         </button>
//       </div>

//       {/* 🏠 HOME */}
//       {activeTab === "home" && (
//         <div className="home">
//           <h2>Welcome 👋</h2>
//           <p>
//             Manage and monitor your data pipelines efficiently.  
//             Use the controls above to trigger runs or interact with the AI assistant.
//           </p>
//         </div>
//       )}

//       {/* 💬 CHAT */}
//       {activeTab === "chat" && (
//         <>
//           <div className="chat-box">
//             {messages.map((msg, index) => (
//               <div
//                 key={index}
//                 className={`message ${
//                   msg.type === "user" ? "user" : "bot"
//                 }`}
//               >
//                 {msg.type === "bot" ? (
//                   <ResponseRenderer text={msg.text} />
//                 ) : (
//                   msg.text
//                 )}
//               </div>
//             ))}

//             {loading && (
//               <div className="message bot loading">Thinking...</div>
//             )}

//             <div ref={chatEndRef} />
//           </div>

//           <div className="input-area">
//             <input
//               type="text"
//               placeholder="Ask something about your pipeline..."
//               value={question}
//               onChange={(e) => setQuestion(e.target.value)}
//               onKeyDown={(e) => e.key === "Enter" && handleSend()}
//             />
//             <button onClick={handleSend}>Send</button>
//           </div>
//         </>
//       )}
//     </div>
//   </div>
// );
// }

// export default App;

// import { useState, useEffect, useRef } from "react";
// import ResponseRenderer from "./ResponseRenderer";
// import logo from "./sigmoid_logo.png";
// import "./App.css";

// const HARDCODED_PIPELINE_ID = "b5b42690-b884-4945-8da6-74b19aa93553";

// function App() {
//   const [question, setQuestion] = useState("");
//   const [messages, setMessages] = useState([]);
//   const [loading, setLoading] = useState(false);
//   const [activeTab, setActiveTab] = useState("home");

//   const chatEndRef = useRef(null);
//   const pollingRef = useRef(null);

//   useEffect(() => {
//     chatEndRef.current?.scrollIntoView({ behavior: "smooth" });
//   }, [messages]);

//   useEffect(() => {
//     return () => {
//       if (pollingRef.current) clearInterval(pollingRef.current);
//     };
//   }, []);

//   const startPolling = () => {
//     if (pollingRef.current) clearInterval(pollingRef.current);

//     pollingRef.current = setInterval(async () => {
//       try {
//         const res = await fetch(`http://localhost:5000/pipeline-status`);
//         const data = await res.json();
//         const status = data.status?.toUpperCase();

//         if (status === "COMPLETED" || status === "FAILED") {
//           clearInterval(pollingRef.current);
//           pollingRef.current = null;

//           setMessages((prev) => [
//             ...prev,
//             {
//               type: "bot",
//               text: `${status === "COMPLETED" ? "✅" : "❌"} **Final Status: ${status}**\n\n${data.details}`,
//             },
//           ]);
//         } else {
//           setMessages((prev) => [
//             ...prev,
//             { type: "bot", text: `📡 Pipeline Status: **${status}** (Syncing Layers)...` },
//           ]);
//         }
//       } catch (err) {
//         console.error("Polling error:", err);
//       }
//     }, 60000);
//   };

//   const handleTrigger = async (endpoint, label) => {
//     setActiveTab("chat");
//     setMessages((prev) => [
//       ...prev,
//       { type: "bot", text: `🚀 Triggering **${label}**...` },
//     ]);

//     try {
//       const res = await fetch(`http://localhost:5000/${endpoint}`, {
//         method: "POST",
//         headers: { "Content-Type": "application/json" },
//         body: JSON.stringify({ question: `Run ${label}` }),
//       });

//       const data = await res.json();
//       setMessages((prev) => [
//         ...prev,
//         { type: "bot", text: data.answer || "Request acknowledged." },
//         { type: "bot", text: "⏳ Waiting for pipeline update to register (5s)..." },
//       ]);

//       setTimeout(() => {
//         startPolling();
//       }, 5000);

//     } catch (error) {
//       setMessages((prev) => [
//         ...prev,
//         { type: "bot", text: `⚠️ Failed to trigger ${label}.` },
//       ]);
//     }
//   };

// const handleSend = async () => {
//   if (!question.trim()) return;
//   const userMessage = { type: "user", text: question };
//   setMessages((prev) => [...prev, userMessage]);
//   setLoading(true);

//   try {
//     const res = await fetch("http://localhost:5000/ask", {
//       method: "POST",
//       headers: { "Content-Type": "application/json" },
//       body: JSON.stringify({ question }),
//     });
//     const data = await res.json();

//     // ✨ BEAUTIFIER LOGIC
//     const formatBotResponse = (text) => {
//       return text
//         // 1. Format Revenue/Large Numbers: 3409535.04 -> $3,409,535
//         .replace(/(\d{5,}\.\d+)/g, (match) => {
//           return new Intl.NumberFormat('en-US', {
//             style: 'currency',
//             currency: 'USD',
//             maximumFractionDigits: 0
//           }).format(parseFloat(match));
//         })
//         // 2. Format Discounts: 0.1899 -> 18.99%
//         .replace(/\b(0\.\d{2,4})\b/g, (match) => {
//           return (parseFloat(match) * 100).toFixed(2) + "%";
//         })
//         // 3. Clean up the "Data Results" header
//         .replace(/📊 Data Results \(\d+ rows\)/g, "### 📊 Performance Metrics");
//     };

//     setMessages((prev) => [
//       ...prev,
//       { type: "bot", text: formatBotResponse(data.answer) }
//     ]);
//   } catch (err) {
//     setMessages((prev) => [...prev, { type: "bot", text: "⚠️ API Error" }]);
//   } finally {
//     setLoading(false);
//     setQuestion("");
//   }
// };

//   return (
//     <div className="app-container">
//       {/* 🛠️ SIDEBAR */}
//       <aside className="sidebar">
//         <div className="logo-section">
//           {/* Wrap the variable 'logo' in curly braces */}
//           <img src={logo} alt="Sigmoid Logo" className="logo-img" />
//           {/* <span className="logo-text">Sigmoid</span> */}
//         </div>

//         <nav className="nav-menu">
//           <p className="menu-label">NAVIGATION</p>
//           <button 
//             className={`nav-btn ${activeTab === 'home' ? 'active' : ''}`} 
//             onClick={() => setActiveTab("home")}
//           >
//             🏠 Home
//           </button>
//           <button 
//             className={`nav-btn ${activeTab === 'chat' ? 'active' : ''}`} 
//             onClick={() => setActiveTab("chat")}
//           >
//             💬 Chat Assistant
//           </button>

//           <p className="menu-label">ACTIONS</p>
//           <button onClick={() => handleTrigger("full-refresh", "Full Refresh")} className="action-btn">
//             🔄 Full Refresh
//           </button>
//           <button onClick={() => handleTrigger("run-pipeline", "Incremental Run")} className="action-btn">
//             ⚡ Run Incremental
//           </button>
//         </nav>
//       </aside>

//       {/* 🖥️ MAIN CONTENT AREA */}
//       <main className="main-content">
//         <header className="main-header">
//           <div className="header-info">
//             <h1 className="system-title">IntelliPipe Control System</h1>
//             {/* <p className="pipeline-id">ID: {HARDCODED_PIPELINE_ID}</p> */}
//           </div>
//         </header>

//         <div className="view-container">
//           {activeTab === "home" && (
//             <div className="home-view">
//               <h2>Welcome 👋</h2>
//               <p>Select an action from the sidebar to manage your Databricks pipeline.</p>
//               <div className="dashboard-placeholder">
//                 {/* Future dashboard cards go here */}
//                 Dashboard Overview
//               </div>
//             </div>
//           )}

//           {activeTab === "chat" && (
//             <div className="chat-interface">
//               <div className="chat-box">
//                 {messages.map((msg, i) => (
//                   <div key={i} className={`message-row ${msg.type}`}>
//                     <div className="bubble">
//                       {msg.type === "bot" ? <ResponseRenderer text={msg.text} /> : msg.text}
//                     </div>
//                   </div>
//                 ))}
//                 {loading && <div className="message-row bot"><div className="bubble loading">Thinking...</div></div>}
//                 <div ref={chatEndRef} />
//               </div>

//               <div className="input-area">
//                 <input
//                   type="text"
//                   placeholder="Type a message..."
//                   value={question}
//                   onChange={(e) => setQuestion(e.target.value)}
//                   onKeyDown={(e) => e.key === "Enter" && handleSend()}
//                 />
//                 <button onClick={handleSend} className="send-btn">Send</button>
//               </div>
//             </div>
//           )}
//         </div>
//       </main>
//     </div>
//   );
// }

// export default App;



import { useState, useEffect, useRef } from "react";
import ResponseRenderer from "./ResponseRenderer";
import logo from "./sigmoid_logo.png"; // Ensure this path is correct
import "./App.css";

const HARDCODED_PIPELINE_ID = "b5b42690-b884-4945-8da6-74b19aa93553";

function App() {
  const [question, setQuestion] = useState("");
  const [messages, setMessages] = useState([]);
  const [loading, setLoading] = useState(false);
  const [activeTab, setActiveTab] = useState("home");

  const chatEndRef = useRef(null);
  const pollingRef = useRef(null);

  useEffect(() => {
    chatEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages]);

  useEffect(() => {
    return () => {
      if (pollingRef.current) clearInterval(pollingRef.current);
    };
  }, []);

  const startPolling = () => {
    if (pollingRef.current) clearInterval(pollingRef.current);
    pollingRef.current = setInterval(async () => {
      try {
        const res = await fetch(`http://localhost:5000/pipeline-status`);
        const data = await res.json();
        const status = data.status?.toUpperCase();

        if (status === "COMPLETED" || status === "FAILED") {
          clearInterval(pollingRef.current);
          pollingRef.current = null;
          setMessages((prev) => [
            ...prev,
            {
              type: "bot",
              text: `${status === "COMPLETED" ? "✅" : "❌"} **Final Status: ${status}**\n\n${data.details}`,
            },
          ]);
        } else {
          setMessages((prev) => [
            ...prev,
            { type: "bot", text: `📡 Pipeline Status: **${status}** (Syncing Layers)...` },
          ]);
        }
      } catch (err) {
        console.error("Polling error:", err);
      }
    }, 60000);
  };

  const handleTrigger = async (endpoint, label) => {
    setActiveTab("chat");
    setMessages((prev) => [...prev, { type: "bot", text: `🚀 Triggering **${label}**...` }]);

    try {
      const res = await fetch(`http://localhost:5000/${endpoint}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ question: `Run ${label}` }),
      });
      const data = await res.json();
      setMessages((prev) => [
        ...prev,
        { type: "bot", text: data.answer || "Request acknowledged." },
        { type: "bot", text: "⏳ Waiting for pipeline update to register (15s)..." },
      ]);
      setTimeout(() => startPolling(), 15000); // 15s delay for Full Refresh safety
    } catch (error) {
      setMessages((prev) => [...prev, { type: "bot", text: `⚠️ Failed to trigger ${label}.` }]);
    }
  };

  const handleSend = async () => {
  if (!question.trim()) return;
  const userMessage = { type: "user", text: question };
  setMessages((prev) => [...prev, userMessage]);
  setLoading(true);

  try {
    const res = await fetch("http://localhost:5000/ask", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ question }),
    });
    const data = await res.json();

    // 🛠️ DATA SANITIZER & BEAUTIFIER
  const formatBotResponse = (text) => {
    if (!text) return "";

    // 🚨 STEP 1: Remove ALL empty lines inside tables
    text = text.replace(/(\|[^\n]+\|)\n\s*\n(?=\|)/g, "$1\n");

    // 🚨 STEP 2: Collapse multiple newlines globally (safe)
    text = text.replace(/\n{2,}/g, "\n\n");

    // 🚨 STEP 3: Fix header separator spacing
    text = text.replace(/\|\s*---/g, "| ---");

    // 💰 Currency formatting
    text = text.replace(/₹?(\d{5,})/g, (match, num) => {
      return "₹" + Number(num).toLocaleString("en-IN");
    });

    // 📊 Percentage formatting
    text = text.replace(/\b(0\.\d+)\b/g, (match) => {
      return (parseFloat(match) * 100).toFixed(2) + "%";
    });

    // 🧠 Section headers
    text = text
      .replace(/📝 SQL Query Executed/g, "### 📝 SQL Query Executed")
      .replace(/📊 Data Results/g, "### 📊 Data Results");

    return text;
  };

    setMessages((prev) => [
      ...prev,
      { type: "bot", text: formatBotResponse(data.answer) }
    ]);
  } catch (err) {
    setMessages((prev) => [...prev, { type: "bot", text: "⚠️ API Communication Error" }]);
  } finally {
    setLoading(false);
    setQuestion("");
  }
};

  return (
    <div className="app-container">
      {/* 🛠️ SIDEBAR - LEFT SIDE */}
      <aside className="sidebar">
        <div className="sidebar-header">
          <h2 className="brand">INTELLIPIPE</h2>
        </div>
        
        <nav className="nav-menu">
          <p className="menu-label">NAVIGATION</p>
          <button className={`nav-btn ${activeTab === 'home' ? 'active' : ''}`} onClick={() => setActiveTab("home")}>🏠 Home</button>
          <button className={`nav-btn ${activeTab === 'chat' ? 'active' : ''}`} onClick={() => setActiveTab("chat")}>💬 Chat Assistant</button>

          <p className="menu-label">ACTIONS</p>
          <button onClick={() => handleTrigger("full-refresh", "Full Refresh")} className="action-btn">🔄 Full Refresh</button>
          <button onClick={() => handleTrigger("run-pipeline", "Incremental Run")} className="action-btn">⚡ Run Incremental</button>
        </nav>
      </aside>

      {/* 🖥️ MAIN CONTENT */}
      <main className="main-content">
        <header className="main-header">
          <div className="header-left">
            <h1 className="system-title">IntelliPipe Control System</h1>
            <p className="pipeline-id">ID: {HARDCODED_PIPELINE_ID}</p>
          </div>
          <div className="header-right">
            <img src={logo} alt="Company Logo" className="top-logo" />
          </div>
        </header>

        <div className="view-container">
          {activeTab === "home" && (
            <div className="home-view">
              <h2>Welcome 👋</h2>
              <p>Select an action from the sidebar to manage your Databricks pipeline.</p>
            </div>
          )}

          {activeTab === "chat" && (
            <div className="chat-interface">
              <div className="chat-box">
                {messages.map((msg, i) => (
                  <div key={i} className={`message-row ${msg.type}`}>
                    <div className="bubble">
                      {msg.type === "bot" ? <ResponseRenderer text={msg.text} /> : msg.text}
                    </div>
                  </div>
                ))}
                {loading && <div className="message-row bot"><div className="bubble loading">Thinking...</div></div>}
                <div ref={chatEndRef} />
              </div>

              <div className="input-area">
                <input
                  type="text"
                  placeholder="Type a message..."
                  value={question}
                  onChange={(e) => setQuestion(e.target.value)}
                  onKeyDown={(e) => e.key === "Enter" && handleSend()}
                />
                <button onClick={handleSend} className="send-btn">Send</button>
              </div>
            </div>
          )}
        </div>
      </main>
    </div>
  );
}

export default App;