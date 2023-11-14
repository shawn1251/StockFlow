# StockFlow
This repository serves as a practice project for utilizing Apache Airflow in conjunction with PostgreSQL for managing and processing stock trading data.

<img src="https://raw.githubusercontent.com/shawn1251/StockFlow/main/demo/upload_e7776ebe5251d901c66d80703eb0415b.png" width="600" />

## Overview
* **Airflow Usage:** This project is a hands-on exploration of Apache Airflow and PostgreSQL, showcasing how they can be integrated for data processing and management tasks.

* **Docker Image Customization:** Leveraging the official Docker images, we have tailored a customized image to meet the specific requirements of this project.

* **Data Collection and Storage:** The primary functionality involves using Yahoo Finance (yfinance) to periodically fetch trading data for target stocks. The acquired data is then stored in a PostgreSQL database. Initially, the data is placed in a staging area, and subsequently, records with the same ticker ID and timestamp are merged.

* **Docker Compose Configuration:** The `docker-compose.yml` file exposes the PostgreSQL service, allowing for easy data integration based on subsequent research needs.

## Workflow Steps

1. **Download Task:** Fetches trading data for target stocks using `yfinance`.
2. **Import Task:** Imports the fetched data into the PostgreSQL database.
3. **Merge SQL Task:** Merges records with the same ticker ID and timestamp in the database.
4. **Archive Task:** Archives or further processes the merged data (task details can be expanded based on project needs).

## Getting Started
To get started with this project, follow these steps:

1. **Clone this repository:**
    ```bash
    git clone 
    cd StockFlow
    ```

2. **Customize the Docker image based on your project requirements.**

3. **Set up and run the Airflow environment:**
    ```bash
    mkdir -p ./dags ./logs ./plugins ./config ./db
    echo -e "AIRFLOW_UID=$(id -u)" > .env
    docker-compose up -d
    ```

4. **Configure the database connection in `Admin>Connections`:**
    Example setting:

   <img src="https://raw.githubusercontent.com/shawn1251/StockFlow/main/demo/upload_b7b68fd5c8f95b4e237233036f43e985.png" width="400" />

5. **Add variables in `Admin>Variables`:**
    Default stocks: `["^GSPC", "^IXIC", "^DJI"]`. You can create your target list in the variable `stock_list`.

   <img src="https://raw.githubusercontent.com/shawn1251/StockFlow/main/demo/upload_c00fdaa3d2b8bd6e722cc04af1af9baa.png" width="400" />

6. **Trigger the defined Airflow tasks to observe the flow in action.**

    <img src="https://raw.githubusercontent.com/shawn1251/StockFlow/main/demo/upload_e7776ebe5251d901c66d80703eb0415b.png" width="600" />

7. **Check the stock data:**
    Because the docker-compose has exposed the PostgreSQL port, we can easily check the data using GUI tools. Here we use pgAdmin4.

    <img src="https://raw.githubusercontent.com/shawn1251/StockFlow/main/demo/upload_8403dbd3644ca977bc0f2e5d2bc66770.png" width="600" />

# Acknowledgements

I would like to express my sincere gratitude to [DataSmiles](https://www.youtube.com/@DataSmiles) for creating an excellent tutorial series on YouTube that greatly contributed to the development of this project. The step-by-step guidance and insights provided in the tutorial were invaluable in helping me understand and implement Apache Airflow. You can find the tutorial series [here](https://www.youtube.com/watch?v=HBgEJzIny2Y&list=PLhJ3QVME-4qC30FA0ylxi4QNQp9FRiSrW).

Thank you for sharing your knowledge and making the learning journey enjoyable and insightful!
