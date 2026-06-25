import json
import socket
import uuid
import logging
from datetime import datetime, timezone
import project_config as cfg

if cfg.getConfig()["msg_size"] is None or cfg.getConfig()['msg_size'] >32768:
    max_msg_size = 32768
else:
    max_msg_size = cfg.getConfig()["msg_size"]
if cfg.getConfig()["ttl"] is None:
    ttl = 7200
else:
    ttl = cfg.getConfig()["ttl"]
if cfg.getConfig()["timeout"] is None:
    timeout = 5
else:
    timeout = cfg.getConfig()["timeout"]

def _send_data(sock: socket.socket, data):
    try:
        # Convert the data to JSON and encode it to bytes
        json_data = json.dumps(data).encode('utf-8') +b'\n'
        if len(json_data) > max_msg_size:
            logging.error(f"message size exceeds the limit of {max_msg_size} bytes.")
            return
        # Send the data
        sock.sendall(json_data)
    except Exception as e:
        socket.close()
        logging.error(f"Error sending data: {e}")

def _wait_for_ack(sock: socket.socket, message_id: str):
    try:
        sock.settimeout(timeout)
        # Wait for the acknowledgment
        ack_data = sock.recv(1024)
        ack_message = json.loads(ack_data.decode('utf-8'))
        if ack_message.get("type") == "ACK" and ack_message.get("message_id") == message_id:
            return True
        else:
            logging.error(f"Received invalid acknowledgment: {ack_message}")
            return False
    except socket.timeout:
        logging.error("Timeout waiting for acknowledgment.")
        return False
    except Exception as e:
        logging.error(f"Error waiting for acknowledgment: {e}")
        return False

def send_message(sock: socket.socket, src: str, dest: str, message: str):
    message_id = str(uuid.uuid4())
    data = {
        "type": "SEND",
        "msg_id": message_id,
        "src": src,
        "dest": dest,
        "payload": message,
        "require_ack": True,
        "ttl": ttl    
    }
    _send_data(sock, data)
    if not _wait_for_ack(sock, message_id):
        logging.error("Failed to receive acknowledgment for the sent message.")

def send_ack(sock: socket.socket, message_id: str):
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    ack_data = {
        "type": "ACK",
        "msg_id": message_id
        "timestamp": timestamp
        "ttl": ttl
    }
    _send_data(sock, ack_data)

def publish_message(sock: socket.socket, src: str, dst: str, message: str):
    message_id = str(uuid.uuid4())
    data = {
        "type": "PUB",
        "msg_id": message_id,
        "src": src,
        "dst": dst,
        "payload": message,
        "require_ack": False,
        "ttl": ttl    
    }
    _send_data(sock, data)

def send_bye (sock: socket.socket, src: str, dest: str, reason: str):
    message_id = str(uuid.uuid4())
    data = {
        "type": "BYE",
        "msg_id": message_id,
        "src": src,
        "dest": dest,
        "reason": reason,
        "ttl": ttl    
    }
    _send_data(sock, data)

def send_bye_ok(sock: socket.socket, src: str, dest: str, msg_id_to_ack: str):
    message_id = str(uuid.uuid4())
    data = {
        "type": "BYE_OK",
        "msg_id_to_ack": msg_id_to_ack,
        "src": src,
        "dest": dest,
        "ttl": ttl    
    }
    _send_data(sock, data)

def handle_incoming_message(sock: socket.socket, cfg.getconfig()["name"]: str):
   try:
    while True:
        # Receive data from the socket
        data = sock.recv(max_msg_size)
        if not data:
            break  # Connection closed
        message = json.loads(data.decode('utf-8'.strip().split('\n')))
        msg_type = message.get("type")
        if msg_type == "SEND":
            logging.info(f"Received SEND message from {message.get('src')}: {message.get('payload')}")
            if message.get("require_ack"):
                send_ack(sock, message.get("msg_id"))
        elif msg_type == "PUB":
         logging.info(f"Received PUB message from {message.get('src')}: {message.get('payload')}")
        elif msg_type == "BYE":
            logging.info(f"Received BYE message from {message.get('src')}: {message.get('reason')}")
            send_bye_ok(sock, cfg.getConfig()["name"], message.get("src"), message.get("msg_id"))
        elif msg_type == "BYE_OK":
            logging.info(f"Received BYE_OK message from {message.get('src')} acknowledging BYE with msg_id: {message.get('msg_id_to_ack')}")
        else:
            logging.warning(f"Received unknown message type: {msg_type}")
  except Exception as e:
    logging.error(f"Error handling incoming message: {e}")
 finally:
    logging.info("Exiting receive loop, closing socket.")
    sock.close()