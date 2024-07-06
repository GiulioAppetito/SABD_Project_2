# Real-time Analysis of Hard Disk Monitoring Events with Apache Flink

## Project Description
The goal of this project is to answer several queries on telemetry data from approximately 200k hard disks in data centers managed by Backblaze, using the stream processing approach with Apache Flink. The dataset contains S.M.A.R.T. monitoring data, extended with additional attributes captured by Backblaze. The reduced dataset contains approximately 3 million events.


## Requirements
- [Docker](https://www.docker.com/)
- [Docker Compose](https://docs.docker.com/compose/)

## Setup and Execution
1. **Ensure Docker and Docker Compose are installed** on your system.
2. **Create a `.env` file** in the root directory of the project if not already present, and define the necessary environment variables. Example:
    ```
    MY_VARIABLE=value
    ANOTHER_VARIABLE=another_value
    ```
3. **Build and start the containers**:
    Navigate to the project directory (where `docker-compose.yml` is located) and execute the following commands:

    ```sh
    docker-compose build
    docker-compose up
    ```

4. **Navigate to the `scripts` directory**:
    ```sh
    cd scripts
    ```

5. **Start the architecture**:
    ```sh
    ./manage-architecture.sh --start
    ```

6. **Start Flink job(s)**:
    ```sh
    ./start-flink.sh --numberQuery
    ```

7. **Start the producer** to begin sending tuples to Kafka:
    ```sh
    ./start-producer.sh
    ```

8. **View the results**:
    The results will be saved in the `Results` directory.

9. **Stop the architecture** when done:
    ```sh
    ./manage-architecture.sh --stop
    ```

## Queries
The following queries will be answered in streaming mode:

- **Query 1**: For vaults with IDs between 1000 and 1020, calculate the number of events, the average, and the standard deviation of the temperature measured on their hard disks.
- **Query 2**: Calculate the real-time top 10 vaults with the highest number of failures within the same day.
- **Query 3**: Calculate the minimum, 25th, 50th, 75th percentile, and maximum of the operating hours for hard disks in vaults with IDs between 1090 and 1120.

## Additional Information
- Ensure the consistency of time intervals by accelerating the dataset replay appropriately.
- Results for each query must be saved in CSV format in the `Results` directory.
- Evaluate latency and throughput of the queries during processing and include the analysis in the project report and presentation.

## Authors
- [Your Name](https://github.com/your-username)

## License
This project is licensed under the MIT License.
