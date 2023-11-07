# Transport for London Bicycle Sharing Data Analysis

This project uses bicycle share data from Transport for London (TfL) to perform data analysis and orchestrate data workflows using Apache Airflow. You can use this repository to set up a development environment for data analysis and workflow management.

## Installation

To get started, follow these steps:

### Prerequisites

Before you begin, make sure you have the following tools installed on your system:

- [Docker](https://www.docker.com/get-started)
- [Docker Compose](https://docs.docker.com/compose/install/)
- [Poetry](https://python-poetry.org/docs/)

### Step 1: Clone the Repository

```bash
git clone https://github.com/a-mtt/tfl-cycle.git
cd tfl-cycle
```

### Step 2: Install Dependencies with Poetry
```bash
poetry install
```

### Step 2: Install Dependencies with Poetry
```bash
poetry install
```

### Step 3: Build and Start Docker Containers
```bash
docker compose up
```
This will build and start the required Docker containers, including the Apache Airflow instance.

### Step 4: Access the Airflow UI
Once the Docker containers are up and running, you can access the Airflow UI by opening your web browser and navigating to:
```bash
http://localhost:8080
```

You should see the Airflow UI, where you can manage and monitor your data workflows.
