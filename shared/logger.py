import logging
import json
import traceback
from datetime import datetime

class JSONFormatter(logging.Formatter):
    def format(self, record):
        log_obj = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": record.levelname,
            "service": getattr(record, "service", "unknown"),
            "message": record.getMessage(),
        }
        
        # Include optional structured fields if they are provided via `extra`
        if hasattr(record, "txn_id"):
            log_obj["txn_id"] = record.txn_id
        if hasattr(record, "event"):
            log_obj["event"] = record.event
        if hasattr(record, "action"):
            log_obj["action"] = record.action
            
        # Add error information if exc_info is present
        if record.exc_info:
            log_obj["error"] = {
                "type": record.exc_info[0].__name__,
                "message": str(record.exc_info[1]),
                "traceback": "".join(traceback.format_exception(*record.exc_info))
            }
            
        return json.dumps(log_obj)

class ServiceLoggerAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        # Merge the extra from kwargs with the adapter's extra
        extra = self.extra.copy()
        if "extra" in kwargs:
            extra.update(kwargs["extra"])
        kwargs["extra"] = extra
        return msg, kwargs

def get_logger(service_name: str, level=logging.INFO) -> logging.Logger:
    logger = logging.getLogger(f"payflow.{service_name}")
    
    # Prevent attaching multiple handlers if get_logger is called multiple times
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = JSONFormatter()
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(level)
        logger.propagate = False
        
    # We use a custom LoggerAdapter to inject the service name and merge extras
    return ServiceLoggerAdapter(logger, extra={"service": service_name})
