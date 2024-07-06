# Real-time Analysis of Hard Disk Monitoring Events with Apache Flink

## Project Description
The goal of this project is to answer several queries on telemetry data from approximately 200k hard disks in data centers managed by Backblaze, using the stream processing approach with Apache Flink. The dataset contains S.M.A.R.T. monitoring data, extended with additional attributes captured by Backblaze. The reduced dataset contains approximately 3 million events.


## Requirements
- [Docker]
- [Docker Compose]

## Setup and Execution
1.**Load dataset**: Move the dataset to `./producer/data` folder, and change the path (if necessary) in the `.env` file.
2. **Navigate to the `scripts` directory**:
    ```sh
    cd scripts
    ```

3. **Start the architecture**:
    ```sh
    ./manage-architecture.sh --start
    ```

4. **Start Flink job(s)**:
   Select between Query1, Query2 or both. You can choose to execute the selected query (queries) with a 1-day, 3-days, or global window (or all of them).
    ```sh
    ./start-flink.sh [--job1|--job2|--both] [--1d|--3d|--all|--all_three]
    ```

5. **Start the producer** to begin sending tuples to Kafka:
    ```sh
    ./start-producer.sh
    ```

6. **View the results**:
    The results will be saved in the `Results` directory.

7. **Stop the architecture** when done:
    ```sh
    ./manage-architecture.sh --stop
    ```

## Queries
The following queries will be answered:

- **Query 1**: For vaults with IDs between 1000 and 1020, calculate the number of events, the average, and the standard deviation of the temperature measured on their hard disks.
- **Query 2**: Calculate the real-time top 10 vaults with the highest number of failures within the same day.
