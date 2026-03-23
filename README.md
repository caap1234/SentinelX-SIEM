<h1 align="center">
  <img src="https://raw.githubusercontent.com/FortAwesome/Font-Awesome/6.x/svgs/solid/shield-halved.svg" alt="SentinelX Logo" width="120" height="120"/>
  <br>
  SentinelX
</h1>

<p align="center">
  <b>A Lightweight, Scalable, and Dockerized SIEM (Security Information and Event Management) built for modern infrastructure.</b>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white" alt="Python">
  <img src="https://img.shields.io/badge/FastAPI-005571?style=for-the-badge&logo=fastapi" alt="FastAPI">
  <img src="https://img.shields.io/badge/Astro-FF5D01?style=for-the-badge&logo=astro&logoColor=white" alt="Astro">
  <img src="https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white" alt="Docker">
  <img src="https://img.shields.io/badge/PostgreSQL-316192?style=for-the-badge&logo=postgresql&logoColor=white" alt="PostgreSQL">
</p>

<p align="center">
  <a href="#features">Features</a> •
  <a href="#architecture">Architecture</a> •
  <a href="#quick-start-zero-touch-installation">Quick Start</a> •
  <a href="#screenshots">Screenshots</a> •
  <a href="README_es.md">🇪🇸 Leer en Español</a>
</p>

---

## 🛡️ Overview

**SentinelX** is a high-performance SIEM platform designed to ingest, parse, normalize, and correlate security logs across your infrastructure. Powered by highly scalable asynchronous workers, it enriches events with GeoIP data and applies real-time anomaly detection rules, offering out-of-the-box alerting and decay mechanisms for entities.

Whether you run a single VPS or a distributed microservices environment, SentinelX gives you observability and security correlations with a **single command installation**.

## ✨ Features

- **🚀 Automated Zero-Touch Deployment**: SentinelX handles its own passwords, `.env` file generation, network mapping, and Nginx proxying via an interactive installer.
- **⚡ Asynchronous Processing**: Architected with decoupled `parsing_workers` and `engine_workers` bridging directly to PostgreSQL. Scales horizontally via Docker.
- **🌍 GeoIP & Entity Enrichment**: Automatically maps IPs to ASNs, countries, and domains for contextual anomalies.
- **📜 Multi-Service Log Normalization**: Native parsers for `Apache`, `Nginx`, `Exim`, `Dovecot`, `SSH`, and `ModSecurity`.
- **🎯 Dynamic Rule Engine**: Time-decay scoring and behavior-based correlations.
- **🖥️ Blazing Fast UI**: Dashboard built with Astro and modern JavaScript for snappy analytics.

---

## 📸 Visual Tour & Screenshots

<p align="center">
  <i>Explore the SentinelX interface: Modern, responsive, and designed for deep security visibility.</i>
</p>

| **1. Executive Dashboard** | **2. Correlated Alerts** |
|:---:|:---:|
| <img src="https://github.com/user-attachments/assets/7aa93035-bccb-434e-a4a1-ba8302b6d8fb" alt="Dashboard Overview" width="100%"> | <img src="https://github.com/user-attachments/assets/7fd10a8e-ad74-4cc9-a823-9485edc13247" alt="Alerts Dashboard" width="100%"> |
| *Real-time activity charts and security KPIs at a glance.* | *Centralized view of detected threats and correlations.* |

| **3. Deep Forensic Detail** | **4. Incident Management** |
|:---:|:---:|
| <img src="https://github.com/user-attachments/assets/b0407ba1-253d-4c9e-9e34-568c34905acf" alt="Alert Details" width="100%"> | <img src="https://github.com/user-attachments/assets/ac772408-84cf-4aa0-8cd1-a908526d3caf" alt="Incident Investigation" width="100%"> |
| *Extensive evidence collection including raw logs and metrics.* | *Full lifecycle case management for active threats.* |

| **5. Entity Intelligence** | **6. Engine Processes** |
|:---:|:---:|
| <img src="https://github.com/user-attachments/assets/a6c7ce6a-59cb-4944-87fa-d97957304e1e" alt="Entity Risk Scoring" width="100%"> | <img src="https://github.com/user-attachments/assets/eda9bd2b-e0ec-43c7-95f7-97b1a675cf53" alt="System Processes" width="100%"> |
| *Behavioral analysis and risk scoring for IPs and users.* | *Monitoring correlation engine health and ingest pipelines.* |

---

## 🏗️ Architecture

SentinelX fundamentally relies on a decoupled Publisher/Consumer methodology, highly abstracted through scalable Docker deployment.

```mermaid
flowchart TD
    A[Servers/Nodes\nSentinelX Agent] -->|Uploads Logs| B(FastAPI Backend)
    B -->|Persists Raw| DB[(PostgreSQL)]
    
    PW[Parsing Workers\nScalable] -->|Fetch Raw Logs| DB
    PW -->|Normalize & Enrich\nGeoIP / ASN| DB
    
    EW[Engine Workers\nCorrelations] -->|Detect Patterns\nScoring & Decay| DB
    EW -->|Generate| C{Alerts & Incidents}
    
    UI[Astro Frontend] <-->|Rest API| B
```

---

## ⚡ Quick Start (Zero-Touch Installation)

Deploying a complex SIEM has never been easier. We provide a tailored, interactive installation script that auto-generates secure passwords, configures Docker Networks, handles Reverse Proxies, and builds the frontend.

**Prerequisites**:
- Linux (Ubuntu/Debian/RHEL/Alma) recommended.
- `Docker` and `Docker Compose (v2)`.

### 1-Command Install

```bash
git clone https://github.com/yourusername/SentinelX-Neubox.git
cd SentinelX-Neubox

# Launch the orchestrator
bash setup_sentinelx.sh
```

**What the script does for you:**
1. Prompts you for a `Local` (Fast Install) or `Server` (Public Domain) environment.
2. Auto-generates cryptographically secure `POSTGRES_PASSWORD`, `SECRET_KEY`, and `INITIAL_ADMIN_PASSWORD`.
3. Handles `GeoLite2` database detection.
4. Spins up a temporary isolated Nginx container if running locally, or configures the Host's Nginx for Public Domains.
5. Scales the asynchronous workers out-of-the-box (`docker compose up --scale parsing_worker=2`).

### Access the Platform
Once the script finishes, check your terminal for the Auto-Generated Admin Credentials.
- **Local Mode:** `http://localhost:4321`
- **Server Mode:** `https://your-configured-domain.com`

---

## 📈 Scalability

SentinelX supports scaling its log-crunching power at any time using Docker Compose:

```bash
# Add more parsing workers for heavy log ingestion
docker compose up -d --scale parsing_worker=4 --scale engine_worker=2
```

---

## 🤝 Contributing

Contributions are always welcome! Whether it's a new log parser, enhanced threat-detection rules, or frontend UI updates:

1. Fork the Project
2. Create your Feature Branch (`git checkout -b feature/AmazingFeature`)
3. Commit your Changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the Branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

---

## 🔒 Security

We take security seriously. 
- Please **never** commit `.env` files or hardcoded credentials. 
- Ensure SentinelX is running behind a secure SSL reverse proxy in production environments.
- Report vulnerabilities to the repository owner directly.

## 📄 License
This project is open-source. See the [LICENSE](LICENSE) file for more details.
