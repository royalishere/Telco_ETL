# PostgreSQL + Power BI Setup Guide

This guide shows how to set up PostgreSQL for your ETL pipeline and connect it to Power BI.

## 🚀 Quick Setup (3 Steps)

### Step 1: Start PostgreSQL
```bash
cd etl/
docker-compose up postgres -d
```
This starts only the PostgreSQL service. Wait 10-15 seconds for it to be ready.

### Step 2: Create Database (One Time Only)
```bash
docker-compose exec postgres psql -U airflow -d postgres -c "CREATE DATABASE telco_data;"
```
This creates the `telco_data` database. You only need to run this once.

### Step 3: Start Airflow
```bash
docker-compose up -d
```
This starts all Airflow services.

## 🔄 Daily Usage

After initial setup, you only need:
```bash
docker-compose up -d          # Start all services
# Run your ETL pipeline in Airflow UI
# Connect Power BI to the data
```

## 📊 Power BI Connection

Once your ETL pipeline has run and loaded data:

**Power BI Desktop → Get Data → PostgreSQL database**

```
Server: localhost:5432
Database: telco_data
Username: airflow  
Password: airflow
Table: telco_customers
```

## 🔧 Troubleshooting

**Database doesn't exist:**
```bash
docker-compose exec postgres psql -U airflow -d postgres -c "CREATE DATABASE telco_data;"
```

**Can't connect to PostgreSQL:**
```bash
docker-compose up postgres -d
# Wait 10-15 seconds for PostgreSQL to start
```

**No data in table:**
- Run the `telco_etl` DAG in Airflow UI (http://localhost:8080)
- Check that CSV files exist in `etl/data/raw/`

**Check if database was created:**
```bash
docker-compose exec postgres psql -U airflow -c "\l"
```

**Check if data loaded successfully:**
```bash
docker-compose exec postgres psql -U airflow -d telco_data -c "SELECT COUNT(*) FROM telco_customers;"
```