from kafka import KafkaConsumer
import json
import psycopg2
from psycopg2 import Error

# Configurações do Kafka
bootstrap_servers = 'localhost:9092'
topic = 'transacoes'

# Configurações do PostgreSQL
postgres_config = {
    'user': 'admin',
    'password': 'admin',
    'host': 'localhost',
    'port': '5432',
    'database': '/transactions'
}

# Criação da conexão com o PostgreSQL
try:
    connection = psycopg2.connect(**postgres_config)
    cursor = connection.cursor()
    print("Conexão com o PostgreSQL bem-sucedida!")
except Error as e:
    print("Erro ao conectar ao PostgreSQL:", e)

# Criação do consumidor Kafka
consumer = KafkaConsumer(topic,
                         group_id='my-group',
                         bootstrap_servers=bootstrap_servers,
                         auto_offset_reset='earliest',
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')))

# Loop para consumir e armazenar transações
for message in consumer:
    transaction = message.value
    print('Transação recebida:', transaction)
    try:
        cursor.execute("INSERT INTO transactions (nome_cliente, banco, bandeira_cartao, valor, local, horario) VALUES (%s, %s, %s, %s, %s, %s)",
                       (transaction['nome_cliente'], transaction['banco'], transaction['bandeira_cartao'], transaction['valor'], transaction['local'], transaction['horario']))
        connection.commit()
        print("Transação armazenada no PostgreSQL!")
    except Error as e:
        print("Erro ao inserir transação no PostgreSQL:", e)

# Fechamento da conexão com o PostgreSQL
cursor.close()
connection.close()
