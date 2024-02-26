from cassandra.cluster import Cluster

cluster = Cluster(['localhost'])
session = cluster.connect()

row = session.execute("select * from stream.users").one()
print(row)