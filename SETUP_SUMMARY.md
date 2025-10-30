In pgAdmin, you need to manually register/add the PostgreSQL server before you can see your database. Unlike Adminer (which connects directly), pgAdmin requires you to configure the server connection first.

Here's how to add it:

Open pgAdmin: http://localhost:8090


Add the server:

Right-click on "Servers" in the left panel
Click "Register" → "Server..."
General Tab:

Name: Data Warehouse (or any name you want)
Connection Tab:

Host: pgdatabase (use the Docker service name, NOT localhost)
Port: 5432
Maintenance database: Trades_Database
Username: postgres
Password: postgres
Save password: ✓ (check this)
Click Save

Important: Use pgdatabase as the host (the Docker service name), not localhost. Inside Docker networks, containers communicate using service names.

After adding the server, you should see your Trades_Database with the integrated_trades_data table inside!

--------------------------------------------------------------------
Access pgAdmin: http://localhost:8090

Email: postgres@postgres.com
Password: postgres
Access Adminer: http://localhost:8080

Server: pgdatabase
Username: postgres
Password: postgres
Database: Trades_Database
