"""
    Módulo de Publicação MQTT para dados de rastreamento.

    Envia dados GPS para um broker MQTT (Mosquitto, EMQX, etc.)
    com suporte a TLS, autenticação e reconexão automática.

"""

import json
import logging
import time
import threading
import platform
from xmlrpc import client
import psutil
from datetime import datetime
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)

# Import condicional do paho-mqtt
try:
    import paho.mqtt.client as mqtt
    HAS_MQTT = True
except ImportError:
    HAS_MQTT = False
    logger.warning("paho-mqtt não instalado. Execute: pip3 install paho-mqtt")


class MQTTManager:
    """Publicador MQTT """

    # QoS níveis MQTT
    QOS_AT_MOST_ONCE  = 0
    QOS_AT_LEAST_ONCE = 1
    QOS_EXACTLY_ONCE  = 2


    def __init__(
        self,
        broker: str = '',
        port: int = 1883,
        client_id: str = '',
        topic_prefix: str = '',
        username: Optional[str] = '',
        password: Optional[str] = '',
        use_tls: bool = False,
        qos: int = 1,
        keepalive: int = 60,
    ):
        """
        Inicializa o publicador MQTT.

        Args:
            broker: Endereço do broker MQTT
            port: Porta do broker (1883 TCP, 8883 TLS)
            client_id: ID do cliente MQTT
            topic_prefix: Prefixo dos tópicos
            username: Nome de usuário para autenticação
            password: Senha para autenticação
            use_tls: Usar TLS/SSL
            qos: Quality of Service (0, 1, 2)
            keepalive: Intervalo keep-alive em segundos
        """
        self.broker = broker
        self.port = port
        if client_id == '':
            self.client_id = f"MQTT-{self.getChipId()}"
        else:
            self.client_id = client_id

        #self.topic_prefix = f"{topic_prefix}{self.getChipId()}"
        self.topic_prefix = f"{topic_prefix}" #Para escutar todos os tópicos, sem prefixo do chip

        self.username = username
        self.password = password
        #self.username = 'usuario'
        #self.password = '1q2w3e!Q@W#E'

        self.use_tls = use_tls
        self.qos = qos
        self.keepalive = keepalive

        self._client = None
        self._connected = False
        self._connect_lock = threading.Lock()
        self._reconnect_delay = 5
        self._max_reconnect_delay = 300
        self._current_reconnect_delay = self._reconnect_delay
        self._publish_queue = []
        self._stats = {
            'published': 0,
            'failed': 0,
            'connected_at': None,
            'disconnected_at': None,
        }

    def connect(self) -> bool:
        """
        Conecta ao broker MQTT.

        Returns:
            True se conectado com sucesso
        """
        if not HAS_MQTT:
            logger.error("paho-mqtt não está instalado")
            return False

        try:
            self._client = mqtt.Client(
                client_id=self.client_id,
                protocol=mqtt.MQTTv311,
                clean_session=True,
            )

            # Configurar callbacks
            self._client.on_connect    = self._on_connect
            self._client.on_disconnect = self._on_disconnect
            self._client.on_publish    = self._on_publish
            self._client.on_log        = self._on_log
            self._client.on_message    = self._on_message

            # Configurar autenticação
            if self.username and self.password:
                self._client.username_pw_set(self.username, self.password)

            # Configurar TLS
            if self.use_tls:
                self._client.tls_set(
                    cert_reqs=mqtt.ssl.CERT_REQUIRED,
                )
                self._client.tls_insecure_set(False)
                if self.port == 1883:
                    self.port = 8883
                logger.info("TLS habilitado para conexão MQTT")

            # Configurar last will (última mensagem)
            will_topic = f"{self.topic_prefix}/AgenteDispFake/status"
            will_payload = json.dumps({
                'status': 'offline',
                'timestamp': datetime.now().isoformat(),
            })
            self._client.will_set(will_topic, will_payload, qos=1, retain=True)

            # Optional: Adjust reconnect delay (min 1 second, max 60 seconds)
            self._client.reconnect_delay_set(min_delay=1, max_delay=60)

            # Conectar
            logger.info(f"Conectando ao broker MQTT: {self.broker}:{self.port}")
            result = self._client.connect(self.broker, self.port, self.keepalive)

            # Iniciar loop em background
            self._client.loop_start()

            # Aguardar conexão
            time.sleep(2)
            return self._connected

        except Exception as e:
            logger.error(f"Erro ao conectar ao broker MQTT: {e}")
            return False

    def disconnect(self):
        """Desconecta do broker MQTT."""
        if self._client:
            # Publicar status offline
            try:
                status_topic = f"{self.topic_prefix}/AgenteDispFake/status"
                payload = json.dumps({
                    'status': 'offline',
                    'timestamp': datetime.now().isoformat(),
                })
                self._client.publish(status_topic, payload, qos=self.qos, retain=True)
            except Exception:
                pass

            self._client.loop_stop()
            self._client.disconnect()
            self._connected = False
            self._stats['disconnected_at'] = datetime.now().isoformat()
            logger.info("Desconectado do broker MQTT")



    # Callbacks MQTT
    def _on_connect(self, client, userdata, flags, rc):
        """Callback de conexão."""
        if rc == 0:
            self._connected = True
            self._current_reconnect_delay = self._reconnect_delay
            self._stats['connected_at'] = datetime.now().isoformat()
            logger.info("Conectado ao broker MQTT com sucesso")

            # Publicar status online
            if self._client:
                topic = f"{self.topic_prefix}/AgenteDispFake/status"
                payload = json.dumps({
                    'status': 'online',
                    'timestamp': datetime.now().isoformat(),
                })
                self._client.publish(topic, payload, qos=self.qos, retain=True)

            self._client.subscribe(f"{self.topic_prefix}/#")

        else:
            self._connected = False
            logger.error(f"Erro de conexão MQTT. Código: {rc}")
            #self._handle_reconnect()

    def _on_disconnect(self, client, userdata, rc):
        """Callback de desconexão."""
        self._connected = False
        self._stats['disconnected_at'] = datetime.now().isoformat()
        logger.warning(f"Desconectado do broker MQTT (rc={rc})")
        #self._handle_reconnect()

    def _on_publish(self, client, userdata, mid):
        """Callback de publicação confirmada."""
        logger.debug(f"Mensagem publicada: mid={mid}")

    def _on_log(self, client, userdata, level, buf):
        """Callback de log."""
        if level == mqtt.MQTT_LOG_ERR:
            logger.error(f"MQTT Log: {buf}")




    # Triggered automatically whenever a message arrives on a subscribed topic
    def _on_message(self, client, userdata, msg):
        # Check if the target substring exists in the incoming message's topic
        if 'FAKE_' in msg.topic and 'response' not in msg.topic:
            try:
                payload = msg.payload.decode('utf-8')
                print(f"[MATCH FOUND] Topic: {msg.topic} | Message: {payload}")

                response_topic = f"{msg.topic}/response"
                self._client.publish(response_topic, 'OK', qos=self.qos, retain=False)
            except UnicodeDecodeError:
                payload = msg.payload
        
        else:
            # Ignore messages that do not match the substring
            pass


    def getChipId(self) -> str:
            """Obtém o ID do chip (MAC address) da interface de rede ativa."""

            ips = []
            redes = []

            interfaces = psutil.net_connections()
            for interface in interfaces:
                if interface.status == 'ESTABLISHED':
                    ips.append(interface.laddr.ip)

            interfaces = psutil.net_if_addrs() # endereços de cada rede
            for interface_name, interface_addresses in interfaces.items():
                mac1 = ''
                ip1 = ''
                for address in interface_addresses:
                    if address.family == psutil.AF_LINK:
                        #print(f"Interface: {interface_name} | MAC: {address.address}")
                        mac1= address.address.replace(':','').replace('-','').upper()
                    if address.family == 2:
                        ip1= address.address
                
                json_data = {
                    "interface": interface_name,
                    "mac": mac1,
                    "ip": ip1
                }
                redes.append(json.dumps(json_data))

            for i in ips:
                for j in redes:
                    if (i == json.loads(j)["ip"]):
                        return json.loads(j)['mac']




    def publish_position(self, data: Dict[str, Any], vehicle_id: str = 'UNKNOWN'):
        """
        Publica dados de posição.

        Args:
            data: Dados GPS (latitude, longitude, altitude, etc.)
            vehicle_id: Identificador do veículo (opcional, padrão 'UNKNOWN')
        """
        if not self._connected:
            logger.warning("Não conectado ao broker MQTT")
            self._stats['failed'] += 1
            return False

        try:
            # Adicionar metadados
            payload = {
                #'vehicle_id': vehicle_id,
                #'timestamp': datetime.now().isoformat(),
                'data': data,
            }

            # Tópico principal de posição
            topic = f"{self.topic_prefix}/position"
            
            msg = json.dumps(payload, default=str)

            result = self._client.publish(topic, msg, qos=self.qos)
            result.wait_for_publish()

            if result.rc == mqtt.MQTT_ERR_SUCCESS:
                self._stats['published'] += 1
                logger.debug(f"Posição publicada: {topic}")
                return True
            else:
                self._stats['failed'] += 1
                logger.error(f"Erro ao publicar posição: rc={result.rc}")
                return False

        except Exception as e:
            logger.error(f"Erro ao publicar posição: {e}")
            self._stats['failed'] += 1
            return False

    def publish_device(self, details: dict = None):
        """
        Publica status do veículo.

        Args:
            vehicle_id: Identificador do veículo
            status: Status (online, offline, error, no_fix)
            details: Detalhes adicionais
        """
        if not self._connected:
            return False

        try:
            payload = {}
            if details:
                payload.update(details)

            topic = f"{self.topic_prefix}/device"

            msg = json.dumps(payload, default=str)
            self._client.publish(topic, msg, qos=self.qos, retain=True)
            return True
        except Exception as e:
            logger.error(f"Erro ao publicar status: {e}")
            return False

    
    
    def publish(self, topic: str, payload: str, qos: int = None, retain: bool = False):
        """
        Publica uma mensagem genérica em um tópico.

        Args:
            topic: Tópico MQTT
            payload: Mensagem (JSON string)
            qos: Quality of Service (0, 1, 2)
            retain: Se a mensagem deve ser retida
        """
        if not self._connected:
            logger.warning("Não conectado ao broker MQTT")
            self._stats['failed'] += 1
            return False

        try:
            if qos is None:
                qos = self.qos

            topicstr = f"{self.topic_prefix}/{topic}"
            result = self._client.publish(topicstr, payload, qos=qos, retain=retain)
            result.wait_for_publish()

            if result.rc == mqtt.MQTT_ERR_SUCCESS:
                self._stats['published'] += 1
                logger.debug(f"Mensagem publicada: {topic}")
                return True
            else:
                self._stats['failed'] += 1
                logger.error(f"Erro ao publicar mensagem: rc={result.rc}")
                return False

        except Exception as e:
            logger.error(f"Erro ao publicar mensagem: {e}")
            self._stats['failed'] += 1
            return False





    @property
    def is_connected(self) -> bool:
        """Verifica se está conectado ao broker."""
        return self._connected


    def get_stats(self) -> dict:
        """Retorna estatísticas de publicação."""
        return dict(self._stats)