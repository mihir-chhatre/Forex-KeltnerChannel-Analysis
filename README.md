# Forex Data Collector and Analyzer

## Description

This project, titled "Forex Data Collector and Analyzer," is designed to fetch real-time foreign exchange (Forex) data, analyze it, and store it in both SQL (SQLite) and NoSQL (MongoDB) databases. The program tracks various currency pairs, calculates statistical data, identifies significant price movements, and stores summarized results for further analysis.

## Features

- Fetches real-time Forex data using the Polygon.io API.
- Stores raw Forex data in SQLite and MongoDB databases.
- Calculates statistical measures (max, min, mean, volatility).
- Detects significant price movements using Keltner Channel-based calculations.
- Periodically updates and clears data in the databases.
- Stores final analyzed data in separate tables/collections in SQLite and MongoDB.

## Requirements

- Python 3.x
- Libraries: `requests`, `time`, `sqlite3`, `pymongo`, `datetime`
- API Key from Polygon.io for Forex data.
- MongoDB server instance running locally.

## Installation

1. Ensure Python 3.x is installed on your system.
2. Install required Python libraries:
    - pip install requests pymongo datetime
3. Set up a local MongoDB instance or use an existing MongoDB server.
4. Obtain an API key from Polygon.io.

## Configuration

- Add your Polygon.io API key to the `api_key` variable in the script.
- Configure the `currency_pairs` list to track desired currency pairs.
- Adjust `duration_hours` for the duration of data collection.
- The script initializes SQLite and MongoDB databases automatically.

## Data Structure

### SQLite Database

- Two databases: `AuxiliaryForexData.db` and `FinalForexData.db`.
- Each currency pair has its own table in both databases.
  - **Auxiliary DB**: Stores raw Forex data.
  - **Final DB**: Stores analyzed data including max, min, mean rates, volatility, and frequency distribution (fd).

### MongoDB Database

- Similar structure to SQLite with two databases: `AuxiliaryForexData` and `FinalForexData`.
- Uses collections for each currency pair.
