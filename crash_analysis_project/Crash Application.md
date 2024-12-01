# Car Crash Application

### Install Required Software

- Java
    - Identify  your processor architecture.
    
    ```java
    # try in terminal
    uname -m
    ```
    
    Output meanings:
    
    - `x86_64` → Download the **x64 Compressed Archive**. (intel chip)
    - `aarch64` or `arm64` → Download the **ARM64 Compressed Archive**. (Apple M1/M2 processor)
    - Why Java?
        - Because spark need Java to run on the local machine.
    - install java 8 or 11 using homebrew, because spark is compatible with java 8 or Java 11.
    - set environment variable to move the java 8 at the permanent directory.
    
- Apache Spark
    - [**https://dlcdn.apache.org/spark/spark-3.4.4/spark-3.4.4-bin-hadoop3.tg](https://dlcdn.apache.org/spark/spark-3.4.4/spark-3.4.4-bin-hadoop3.tgz)z**
    - run spark-shell
- Python 3.13.0
    - MacOS typically has python.
    - But for compatible with VS code, I lower down to python 3.9.x
    

## Key Files and Their Responsibilities

- configs/config.yaml
    - **Purpose**: Stores file paths for input datasets and descriptions of the analyses.
- data/*
    - Contains all the source csv files.
- results/*
    - contains all the resultant analysis csv files.
- scripts/analysis.py
    - Contains analysis logic.
- scripts/app.py
    - The entry point of the application.
- scripts/data_loader.py
    - Loads CSV files into Spark DataFrames.
- scripts/config.py
    - Reads and parses the configuration file.

## To run the project

1-  **Clone the repository**:

```java
git clone https://github.com/vividmansi/car_crash_analysis.git
cd car_crash_analysis

```

2- **Install dependencies**:

```java
pip install -r requirements.txt
```

3- **Run the project, here you go :)**

```java
spark-submit scripts/app.py
```