import cx_Oracle

#connessione al db

#formato connectionString: <username>/<password>@<dbHostAddress>:<dbPort>/<dbServiceName>
conStr = 'system/oracle@localhost:1521/orcl'
conn = cx_Oracle.connect(conStr)
