from prometheus_client import Counter, Histogram

bank_requests_total = Counter('bank_requests_total', 'Total bank operations', ['operation', 'status'])
bank_operation_duration_seconds = Histogram('bank_operation_duration_seconds', 'Latency of bank operations', ['operation'])
