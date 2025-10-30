# Data Engineering Project - Docker Setup

## 📦 What's Included

This Docker setup provides:
- **PostgreSQL 16**: Data warehouse database
- **pgAdmin**: Web-based PostgreSQL management tool
- **Adminer**: Lightweight database management interface (bonus)

## 🚀 Getting Started

### Prerequisites
- Docker Desktop installed
- Docker Compose installed

### Quick Start

1. **Start all services:**
   ```bash
   docker-compose up -d
   ```

2. **Check services are running:**
   ```bash
   docker-compose ps
   ```

3. **View logs:**
   ```bash
   docker-compose logs -f
   ```

## 🔌 Access Points

### PostgreSQL Database
- **Host:** localhost
- **Port:** 5432
- **Database:** trading_warehouse
- **Username:** postgres
- **Password:** postgres

**Connection String:**
```
postgresql://postgres:postgres@localhost:5432/trading_warehouse
```

### pgAdmin (Primary DB Tool)
- **URL:** http://localhost:5050
- **Email:** admin@admin.com
- **Password:** admin

**To add PostgreSQL server in pgAdmin:**
1. Open http://localhost:5050
2. Right-click "Servers" → "Register" → "Server"
3. General Tab:
   - Name: Data Warehouse
4. Connection Tab:
   - Host: postgres (use service name, not localhost)
   - Port: 5432
   - Database: trading_warehouse
   - Username: postgres
   - Password: postgres

### Adminer (Bonus Alternative)
- **URL:** http://localhost:8080
- **System:** PostgreSQL
- **Server:** postgres
- **Username:** postgres
- **Password:** postgres
- **Database:** trading_warehouse

## 🛠️ Common Commands

### Start services
```bash
docker-compose up -d
```

### Stop services
```bash
docker-compose down
```

### Stop and remove all data (fresh start)
```bash
docker-compose down -v
```

### Restart specific service
```bash
docker-compose restart postgres
```

### View service logs
```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f postgres
```

### Execute SQL directly
```bash
docker-compose exec postgres psql -U postgres -d trading_warehouse
```

### Backup database
```bash
docker-compose exec postgres pg_dump -U postgres trading_warehouse > backup.sql
```

### Restore database
```bash
docker-compose exec -T postgres psql -U postgres -d trading_warehouse < backup.sql
```

## 📁 Project Structure

```
Milestone_1/
├── docker-compose.yml          # Docker services configuration
├── Dockerfile                  # Python application container
├── requirements.txt            # Python dependencies
├── init.sql                    # Database initialization script
├── data_cleaning.ipynb         # Data processing notebook
├── datasets/                   # CSV data files
│   ├── trades.csv
│   ├── dim_customer.csv
│   ├── dim_stock.csv
│   ├── dim_date.csv
│   └── daily_trade_prices.csv
└── DOCKER_README.md           # This file
```

## 🔧 Configuration

### Environment Variables

You can customize the setup by editing `docker-compose.yml`:

**PostgreSQL:**
- `POSTGRES_USER`: Database user
- `POSTGRES_PASSWORD`: Database password
- `POSTGRES_DB`: Database name

**pgAdmin:**
- `PGADMIN_DEFAULT_EMAIL`: Login email
- `PGADMIN_DEFAULT_PASSWORD`: Login password

**Ports:**
- PostgreSQL: 5432 (change in ports section)
- pgAdmin: 5050
- Adminer: 8080

### Persistent Data

Data is stored in Docker volumes:
- `postgres_data`: Database files
- `pgadmin_data`: pgAdmin configuration

To remove all data:
```bash
docker-compose down -v
```

## 🐍 Using Python with Docker

### Connect from Python script
```python
from sqlalchemy import create_engine

# Connection string
DATABASE_URL = "postgresql://postgres:postgres@localhost:5432/trading_warehouse"

# Create engine
engine = create_engine(DATABASE_URL)

# Load data
import pandas as pd
df = pd.read_sql("SELECT * FROM integrated_data LIMIT 10", engine)
```

### Load data to PostgreSQL
```python
# Load your integrated dataframe
integrated.to_sql(
    'integrated_data',
    engine,
    if_exists='replace',
    index=False,
    chunksize=1000
)
```

## 🔍 Troubleshooting

### Port already in use
If port 5432 is already in use:
```bash
# Find process using port
netstat -ano | findstr :5432

# Change port in docker-compose.yml
ports:
  - "5433:5432"  # Use 5433 instead
```

### Can't connect to database
1. Check services are running: `docker-compose ps`
2. Check logs: `docker-compose logs postgres`
3. Verify connection from container:
   ```bash
   docker-compose exec postgres psql -U postgres -d trading_warehouse -c "SELECT 1"
   ```

### Reset everything
```bash
docker-compose down -v
docker-compose up -d
```

## 📊 Loading Your Data

After starting the containers, run your Python notebook to load data:

```python
from sqlalchemy import create_engine
import pandas as pd

# Create connection
engine = create_engine('postgresql://postgres:postgres@localhost:5432/trading_warehouse')

# Load your integrated data
integrated.to_sql(
    'integrated_data',
    engine,
    if_exists='replace',
    index=False,
    chunksize=1000
)

print("✅ Data loaded successfully!")
```

## 🎯 Next Steps

1. Start containers: `docker-compose up -d`
2. Open pgAdmin: http://localhost:5050
3. Add PostgreSQL server connection
4. Run your data cleaning notebook
5. Load data into PostgreSQL
6. Query and visualize in pgAdmin or Adminer

## 📝 Notes

- **pgAdmin** is more feature-rich and professional
- **Adminer** is lightweight and simple (bonus option)
- Both tools are included - use whichever you prefer
- Data persists between container restarts
- Use `docker-compose down -v` only when you want to delete all data

## 🆘 Support

For issues:
1. Check logs: `docker-compose logs`
2. Verify services: `docker-compose ps`
3. Restart services: `docker-compose restart`
4. Fresh start: `docker-compose down -v && docker-compose up -d`
