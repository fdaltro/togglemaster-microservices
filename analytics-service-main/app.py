import os
import sys
import threading
import json
import uuid
import time
import logging
import boto3
from botocore.exceptions import NoCredentialsError, ClientError
from flask import Flask, jsonify
from dotenv import load_dotenv

# --- 1. IMPORTAÇÕES DA CAMADA DE OBSERVABILIDADE (TRACES E METRICS) ---
from opentelemetry import trace, metrics
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

# Novos imports para o Motor de Métricas
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter

from opentelemetry.sdk.resources import Resource
from opentelemetry.propagate import set_global_textmap
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

# --- 2. IMPORTAÇÕES DOS INSTRUMENTADORES NATIVOS ---
from opentelemetry.instrumentation.flask import FlaskInstrumentor
from opentelemetry.instrumentation.botocore import BotocoreInstrumentor # Instrumenta SQS e DynamoDB

# Configura o logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)

# Carrega .env para desenvolvimento local
load_dotenv()

# --- 3. CONFIGURAÇÃO DE RECURSOS (NOME DO SERVIÇO) ---
otel_endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://otel-collector.observabilidade.svc.cluster.local:4317")
otel_service = os.getenv("OTEL_SERVICE_NAME", "analytics-service")

resource = Resource.create(attributes={
    "service.name": otel_service,
    "deployment.environment": "prod"
})

# --- 4. CONFIGURAÇÃO DO TRACER GLOBAL (Rastreamento) ---
set_global_textmap(TraceContextTextMapPropagator())

provider = TracerProvider(resource=resource)
processor = BatchSpanProcessor(OTLPSpanExporter(endpoint=otel_endpoint, insecure=True))
provider.add_span_processor(processor)
trace.set_tracer_provider(provider)

# Inicializa o rastreador especializado para processamento assíncrono
tracer = trace.get_tracer("sqs-worker-tracer")

# --- 5. CONFIGURAÇÃO DO METER GLOBAL (Métricas) ---
# Exporta as métricas a cada 5 segundos (5000ms) seguindo o padrão que fizemos no Go
metric_reader = PeriodicExportingMetricReader(
    OTLPMetricExporter(endpoint=otel_endpoint, insecure=True),
    export_interval_millis=5000
)
meter_provider = MeterProvider(resource=resource, metric_readers=[metric_reader])
metrics.set_meter_provider(meter_provider)

# Cria um medidor customizado para o worker do SQS
meter = metrics.get_meter("sqs-worker-meter")
messages_processed_counter = meter.create_counter(
    "sqs_messages_processed_total",
    description="Total de mensagens SQS processadas com sucesso ou erro"
)

# --- 6. ATIVAÇÃO DAS EXTENSÕES AUTOMÁTICAS ---
app = Flask(__name__)
FlaskInstrumentor().instrument_app(app)
BotocoreInstrumentor().instrument() # Intercepta o ciclo de vida do Boto3 automaticamente

# --- Configuração AWS ---
AWS_REGION = os.getenv("AWS_REGION")
SQS_QUEUE_URL = os.getenv("AWS_SQS_URL")
DYNAMODB_TABLE_NAME = os.getenv("AWS_DYNAMODB_TABLE")

if not all([AWS_REGION, SQS_QUEUE_URL, DYNAMODB_TABLE_NAME]):
    log.critical("Erro: AWS_REGION, AWS_SQS_URL, e AWS_DYNAMODB_TABLE devem ser definidos.")
    sys.exit(1)

# --- Clientes Boto3 ---
try:
    session = boto3.Session(region_name=AWS_REGION)
    sqs_client = session.client("sqs")
    dynamodb_client = session.client("dynamodb")
    log.info(f"Clientes Boto3 inicializados na região {AWS_REGION}")
except NoCredentialsError:
    log.critical("Credenciais da AWS não encontradas. Verifique seu ambiente.")
    sys.exit(1)
except Exception as e:
    log.critical(f"Erro ao inicializar o Boto3: {e}")
    sys.exit(1)


# --- SQS Worker ---

def process_message(message):
    """ Processa uma única mensagem SQS e a insere no DynamoDB reconstruindo o rastro OTel """
    
    # RECONSTRUÇÃO DO CONTEXTO: Extrai os cabeçalhos injetados pelo Go de dentro dos MessageAttributes
    carrier = {}
    if 'MessageAttributes' in message:
        for key, attr in message['MessageAttributes'].items():
            if 'StringValue' in attr:
                carrier[key] = attr['StringValue']
                carrier[key.lower()] = attr['StringValue']

    parent_context = TraceContextTextMapPropagator().extract(carrier=carrier)

    with tracer.start_as_current_span("process_sqs_message", context=parent_context) as span:
        try:
            log.info(f"Processando mensagem ID: {message['MessageId']}")
            body = json.loads(message['Body'])
            
            span.set_attribute("messaging.message_id", message['MessageId'])
            span.set_attribute("feature_flag.name", body.get('flag_name', 'unknown'))
            span.set_attribute("messaging.destination", SQS_QUEUE_URL.split('/')[-1])
            
            event_id = str(uuid.uuid4())
            item = {
                'event_id': {'S': event_id},
                'user_id': {'S': body['user_id']},
                'flag_name': {'S': body['flag_name']},
                'result': {'BOOL': body['result']},
                'timestamp': {'S': body['timestamp']}
            }
            
            dynamodb_client.put_item(
                TableName=DYNAMODB_TABLE_NAME,
                Item=item
            )
            
            log.info(f"Evento {event_id} (Flag: {body['flag_name']}) salvo no DynamoDB.")
            
            # 🔥 MÉTRICA: Incrementa sucesso
            messages_processed_counter.add(1, {"status": "success", "flag_name": body.get('flag_name', 'unknown')})
            
            sqs_client.delete_message(
                QueueUrl=SQS_QUEUE_URL,
                ReceiptHandle=message['ReceiptHandle']
            )
            
        except json.JSONDecodeError as e:
            log.error(f"Erro ao decodificar JSON da mensagem ID: {message['MessageId']}")
            span.record_exception(e)
            messages_processed_counter.add(1, {"status": "error_decode", "flag_name": "unknown"})
        except ClientError as e:
            log.error(f"Erro do Boto3 (DynamoDB ou SQS) ao processar {message['MessageId']}: {e}")
            span.record_exception(e)
            messages_processed_counter.add(1, {"status": "error_boto3", "flag_name": body.get('flag_name', 'unknown')})
        except Exception as e:
            log.error(f"Erro inesperado ao processar {message['MessageId']}: {e}")
            span.record_exception(e)
            messages_processed_counter.add(1, {"status": "error_generic", "flag_name": "unknown"})

def sqs_worker_loop():
    log.info("Iniciando o worker SQS...")
    while True:
        try:
            response = sqs_client.receive_message(
                QueueUrl=SQS_QUEUE_URL,
                MaxNumberOfMessages=10,
                WaitTimeSeconds=20,
                MessageAttributeNames=['*'] 
            )
            
            messages = response.get('Messages', [])
            if not messages:
                continue
                
            log.info(f"Recebidas {len(messages)} mensagens.")
            
            for message in messages:
                process_message(message)
                
        except ClientError as e:
            log.error(f"Erro do Boto3 no loop principal do SQS: {e}")
            time.sleep(10)
        except Exception as e:
            log.error(f"Erro inesperado no loop principal do SQS: {e}")
            time.sleep(10)

# --- Servidor Flask ---

@app.route('/health')
def health():
    return jsonify({"status": "ok"})

def start_worker():
    worker_thread = threading.Thread(target=sqs_worker_loop, daemon=True)
    worker_thread.start()

start_worker()

if __name__ == '__main__':
    port = int(os.getenv("PORT", 8005))
    app.run(host='0.0.0.0', port=port, debug=False)