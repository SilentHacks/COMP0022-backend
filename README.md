# Backend API for COMP0022

This backend API, built with Python and FastAPI, interfaces with a PostgreSQL database to fetch and provide movie statistics data. It's containerized with Docker for simplified deployment and development workflows.

## Features

- PostgreSQL for data storage with advanced setup including repmgr and pgpool.
- Easy setup with Docker.
- FastAPI for efficient, asynchronous API requests.

## Getting Started

### Prerequisites

- Docker installed on your machine.

### Installation and Running

1. Clone the repository:
    ```bash
    git clone https://github.com/SilentHacks/COMP0022-backend.git
    ```
   (Though technically you shouldn't be using this repo standalone)
2. Navigate to the repository:
    ```bash
    cd COMP0022-backend
    ```
3. Build the Docker image:
    ```bash
    docker build -t comp0022-backend .
    ```
4. Run the Docker container:
    ```bash
    docker run -d -p 8000:8000 comp0022-backend
    ```

The API will be accessible at http://localhost:8000.

## Usage

Access the API documentation at http://localhost:8000/docs to explore available endpoints and their functionalities.