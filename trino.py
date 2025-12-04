from trino.dbapi import connect

conn = connect(
 host = 'trino',
 port = 8080,
 user = 'dwiputraem',
 catalog = 'iceberg',
 schema = 'default',
)

cur = conn.cursor()
cur.execute('SELECT * FROM iceberg.default.belajar LIMIT 10')
rows = cur.fetchall()
print(rows)
