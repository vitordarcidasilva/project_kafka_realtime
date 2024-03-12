from kafka import KafkaProducer
import json
import random
import time

# Configurações do Kafka
bootstrap_servers = 'localhost:9092'
topic = 'transacoes'

# Criação do produtor Kafka
producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Função para gerar transações aleatórias
def generate_transaction():
    return {
        'nome_cliente': 'Cliente_' + str(random.randint(1, 100)),
        'banco': 'Banco_' + str(random.randint(1, 5)),
        'bandeira_cartao': random.choice(['Visa', 'Mastercard', 'American Express']),
        'valor': round(random.uniform(10.0, 500.0), 2),
        'local': 'Loja_' + str(random.randint(1, 20)),
        'horario': time.strftime("%Y-%m-%d %H:%M:%S")
    }

# Loop para gerar e enviar transações
while True:
    transaction = generate_transaction()
    producer.send(topic, value=transaction)
    producer.flush()
    print('Transação enviada:', transaction)
    time.sleep(1)  # Simula transações sendo geradas a cada segundo
