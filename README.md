# MLOps Batch Processing Pipeline

## 📖 Project Overview
This repository contains a production-grade, containerized batch processing pipeline designed for time-series financial data. The pipeline ingests historical market data, calculates a rolling mean based on a configurable window, and generates binary trading signals.

**Signal Logic:** * `1` if the closing price strictly exceeds the rolling mean.
* `0` otherwise (or if the rolling window is incomplete).

The project emphasizes MLOps best practices, including strict data validation, deterministic execution (via configurable random seeds), robust error handling, and comprehensive observability (logging and JSON metrics).

---

## 📂 Project Structure
* `run.py`: The core execution script containing the data processing logic and CLI.
* `config.yaml`: Configuration file for defining the random seed, window size, and version.
* `data.csv`: The input dataset containing time-series market data.
* `Dockerfile`: Containerization instructions to build an isolated execution environment.
* `requirements.txt`: Minimal external dependencies (`pandas`, `PyYAML`, `numpy`).

---

## 🚀 Execution Instructions (Docker)

The application is fully containerized to ensure reproducibility across different environments. The Docker container is configured to automatically execute the pipeline with the provided dataset and configuration.

### 1. Build the Docker Image
Ensure Docker Desktop is running on your system. Run the following command in the root directory of the project:

```bash
docker build -t mlops-task .
```

### 2. Run the Container
Execute the container. The `--rm` flag ensures the container is cleaned up immediately after execution:

```bash
docker run --rm mlops-task
```

---

## 💻 Local Execution (Without Docker)

If you wish to test the script locally without Docker, ensure you have Python 3.9+ installed.

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Run the pipeline via the Command Line Interface (CLI):
```bash
python run.py --input data.csv --config config.yaml --output metrics.json --log-file run.log
```

---

## 📊 Expected Outputs & Observability

### 1. Standard Output (stdout)
Upon successful execution, the pipeline exits with code `0` and prints a strictly formatted JSON metrics summary directly to the terminal screen.

**Example Success Output:**
```json
{
    "version": "v1",
    "rows_processed": 10000,
    "metric": "signal_rate",
    "value": 0.4989,
    "latency_ms": 63,
    "seed": 42,
    "status": "success"
}
```

### 2. File Artifacts
The script physically generates two artifacts during the run:
* `metrics.json`: A saved copy of the final JSON metrics output.
* `run.log`: A detailed execution log containing step-by-step timestamps, configuration states, data loading confirmations, and processing updates.

---

## 🛡️ Error Handling & Reliability
The pipeline is built to fail gracefully if it encounters malformed datasets or missing configurations. 

If a critical error occurs (e.g., missing 'close' column in CSV), the pipeline will:
1. Log the exact failure reason to `run.log`.
2. Generate an error-formatted JSON payload printed to `stdout`.
3. Force a **non-zero exit code (`1`)** to properly alert the Docker engine or external orchestration tools that the job failed.

**Example Error Output:**
```json
{
    "version": "v1",
    "status": "error",
    "error_message": "Data validation failed: 'close' column is missing."
}
```

---

## ⚙️ Design Choices & Best Practices
* **Vectorized Processing:** Leverages Pandas' C-optimized vectorization (`df['close'].rolling()`) instead of slow, iterative `for` loops to ensure high computational efficiency.
* **Separation of Concerns:** Core functionalities (Config Loading, Data Validation, Processing, Metrics Generation) are isolated into distinct functions for high modularity and testability.
* **Secure YAML Loading:** Utilizes `yaml.safe_load()` to prevent arbitrary code execution vulnerabilities.
* **CLI Argument Parsing:** Uses Python's built-in `argparse` to eliminate hardcoded paths and make the pipeline dynamically configurable at runtime.