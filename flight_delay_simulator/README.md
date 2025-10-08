# 🛫 Flight Robustness — Monte Carlo Simulator for Cascading Flight Delays

A production-style Python project that simulates **reactionary (cascading) flight delays** across an aircraft’s day of flying.  
It reads a scheduled flight plan (CSV), runs **Monte Carlo simulations** with configurable Normal-distributed delays, and produces:

1. **Modified schedule** with actual times per run (`modified_input_with_ATA.csv`)
2. **Aggregated results** across all runs (`aggregated.csv`)
3. *(Optional)* **Histogram plot** (`arrival_delay_distribution.png`) showing delay distribution

---

## 📖 Table of Contents
1. [Overview](#-overview)
2. [Features](#-features)
3. [Architecture](#-architecture)
4. [Tech Stack](#-tech-stack)
5. [Project Structure](#-project-structure)
6. [Installation](#-installation)
7. [Configuration & File Locations](#-configuration--file-locations)
8. [Running the Application](#-running-the-application)
   - [CLI Mode](#cli-mode)
   - [API Mode (FastAPI)](#api-mode-fastapi)
9. [Testing](#-testing)
10. [Docker & Docker Compose](#-docker--docker-compose)
11. [Kubernetes Deployment](#-kubernetes-deployment)
12. [CI/CD Integration](#-cicd-integration)
13. [Troubleshooting](#-troubleshooting)
14. [Contributing](#-contributing)
15. [License](#-license)

---

## 🧠 Overview

This project simulates **cascading flight delays** to help airlines assess operational robustness using Monte Carlo methods.  
It started as a research proof-of-concept and evolved into a **production-grade Python application** featuring a CLI, REST API, and Docker/Kubernetes deployment pipeline.

---

## ✨ Features

- 🔁 Monte Carlo simulation for reactionary flight delays  
- 🧮 Configurable Normal distributions for delay modeling  
- 🧠 Cascading logic between consecutive flight legs  
- 📊 Aggregated statistics and optional delay histogram  
- ⚙️ Configurable via YAML or API payloads  
- 🧱 Modular, testable, and CI-ready structure  
- 🐳 Docker Compose setup for API + CLI  
- ☸️ Kubernetes manifests for scaling (basic skeleton provided)  
- ✅ 100% passing test suite with `pytest`

---

## 🧩 Architecture

```mermaid
graph TD
    CLI[CLI Interface] --> CORE
    API[FastAPI REST API] --> CORE
    CORE[Simulation Engine] --> IO[IO Layer (Reader/Writer/Adapters)]
    IO --> STORAGE[Storage Adapters: Local/S3]
    CORE --> RESULTS[Aggregated Results + Visualizations]
```

---

## 🧱 Tech Stack

| Component | Technology |
|------------|-------------|
| Language | Python 3.10+ |
| API Framework | FastAPI |
| CLI | argparse / Poetry entry point |
| Data Processing | Pandas + Polars |
| Config Mgmt | YAML + `.env` |
| Testing | pytest + httpx |
| Packaging | Poetry |
| Containerization | Docker + Docker Compose |
| Orchestration | Kubernetes (Minikube/EKS) |
| CI/CD | GitHub Actions |

---

## 📁 Project Structure

```
.
├── config.yaml
├── .env
├── data/
│   ├── input/                # Place your input CSV files here
│   │   └── schedule_2.csv
│   ├── results/              # Simulation results are saved here
│   │   ├── modified_input_with_ATA.csv
│   │   └── aggregated.csv
│   └── api/uploads/          # Temporary uploaded files (API mode)
├── src/flightrobustness/
│   ├── core/
│   ├── io/
│   ├── interfaces/
│   └── utils/
├── tests/
├── Dockerfile
├── docker-compose.yml
├── k8s/
│   ├── namespace.yaml
│   ├── configmap.yaml
│   ├── deployment.yaml
│   ├── service.yaml
│   └── ingress.yaml
└── pyproject.toml
```

---

## ⚙️ Installation

### Prerequisites

| Tool | Version | Purpose |
|------|----------|----------|
| Python | 3.10+ | Core runtime |
| Poetry | 1.8+ | Dependency management |
| Docker | 24+ | Container builds |
| Minikube | 1.32+ | Local Kubernetes |
| Kubectl | 1.29+ | Cluster management |

### Setup

```bash
git clone <repo-url>
cd flight_delay_simulator
poetry env use 3.10
poetry install
```

PyCharm users (recommended):

```bash
poetry config virtualenvs.in-project true
poetry install
```

Verification:
```bash
poetry run python -V
poetry run pytest -v
```

---

## ⚙️ Configuration & File Locations

### `config.yaml` (located in project root)

The YAML configuration defines how the simulation behaves.

```yaml
mode: monte_carlo
n_runs: 5
min_turnaround: 45
delays:
  departure: {mean: 10, std: 3}
  inflight: {mean: 5, std: 2}
input_schedule: data/input/schedule_2.csv
output_dir: data/results
plot: true
```

### `.env` (also in project root)

```bash
APP_ENV=local
STORAGE_BACKEND=local
LOCAL_DATA_DIR=./data
AWS_REGION=eu-central-1
S3_INPUT_BUCKET=flight-robustness-input
S3_OUTPUT_BUCKET=flight-robustness-output
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
```

### 📂 File Locations Summary

| Type | Description | Location |
|------|--------------|----------|
| **Config File** | Simulation configuration | `config.yaml` (root directory) |
| **Input CSV** | Upload or place flight schedule here | `data/input/` |
| **Simulation Outputs** | Aggregated and modified CSVs | `data/results/` |
| **API Uploads** | Temporary uploads during API runs | `data/api/uploads/` |

---

## 🧮 Running the Application

### CLI Mode

```bash
poetry run simulate-cli --config config.yaml
```

Optional arguments:

```bash
poetry run simulate-cli --config config.yaml --mode monte_carlo --runs 10
poetry run simulate-cli --config config.yaml --aircraftid LHF32Q_0158
```

---

### API Mode (FastAPI)

Start API locally:
```bash
poetry run uvicorn flightrobustness.interfaces.api:app --host 0.0.0.0 --port 8000 --reload
```

Swagger UI:
👉 [http://127.0.0.1:8000/docs](http://127.0.0.1:8000/docs)

---

## 🧪 Testing

```bash
poetry run pytest -v
poetry run pytest --cov=src/flightrobustness --cov-report=term-missing
```

All 25/25 tests pass ✅

---

## 🐳 Docker & Docker Compose

Build image:
```bash
docker build -t flightrobustness:latest .
```

Run API:
```bash
docker run --rm -p 8000:8000 -v "$PWD/data:/app/data" flightrobustness:latest   poetry run uvicorn flightrobustness.interfaces.api:app --host 0.0.0.0 --port 8000
```

Run CLI:
```bash
docker run --rm -v "$PWD/data:/app/data" flightrobustness:latest   poetry run simulate-cli --config config.yaml
```

> ⚠️ Basic Docker Compose and Kubernetes skeletons are included but **not fully tested due to time constraints**.  
> With more time, a live FastAPI + CLI integration demo could be presented during the interview.

---

## ☸️ Kubernetes Deployment

### Steps
```bash
minikube start --cpus=4 --memory=6g
eval $(minikube docker-env)
docker build -t flightrobustness:local .
kubectl apply -f k8s/
minikube service flightrobustness-service -n poc --url
```

Access API docs:
```
http://127.0.0.1:<port>/docs
```

---

## ✅ Summary of Commands

| Task | Command |
|------|----------|
| Run CLI | `poetry run simulate-cli --config config.yaml` |
| Run API | `poetry run uvicorn flightrobustness.interfaces.api:app --reload` |
| Run Tests | `poetry run pytest -v` |
| Build Docker | `docker build -t flightrobustness .` |
| Run Docker Compose | `docker compose up` |
| Run on K8s | `kubectl apply -f k8s/` |
| View Results | `data/results/` |
| Upload Inputs | `data/input/` |
| Config File | `config.yaml` |

---
