-- Initial database setup script
-- This script runs when the PostgreSQL container is first created

-- Create schema if needed
CREATE SCHEMA IF NOT EXISTS public;

-- You can add initial table creation here if needed
-- For now, tables will be created by the Python application

-- Grant permissions
GRANT ALL PRIVILEGES ON SCHEMA public TO postgres;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO postgres;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO postgres;

-- Log initialization
DO $$
BEGIN
    RAISE NOTICE 'Database trading_warehouse initialized successfully';
END $$;
