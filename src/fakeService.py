"""
Módulo Principal do Sistema.

Orquestra a leitura do GPS via serial, parsing NMEA e publicação MQTT.
Suporta execução como daemon (serviço systemd) ou modo interativo.

"""

import json
import logging
import signal
import sys
import time
import configparser
import os
from datetime import datetime
from typing import Optional

from dotenv import load_dotenv

# Load variables from the .env file into the system environment
load_dotenv()

#
#from src.mqtt_Manager import MQTTManager
from mqtt_Manager import MQTTManager

# Adicionar diretório raiz ao path
#sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


logger = logging.getLogger(__name__)

# Versão do projeto
VERSION = "1.0.0"


class FAKEResponser:
    """
    Serviço principal.
    """

    def __init__(self, config_path: Optional[str] = None):
        """
        Inicializa.

        Args:
            config_path: Caminho para arquivo de configuração .INI
        """

        self.config_ini = configparser.ConfigParser()
        self.config_path = config_path

        # Carregar configuração
        if config_path and os.path.exists(config_path):
            self.config_ini.read(config_path)
            logger.info(f"Configuração carregada de: {config_path}")
        else:
            self._set_defaults()

        
        self.mqtt_manager = None

        # Estado
        self._running = False


    def _set_defaults(self):
        """Define configurações padrão."""

        self.config_ini['MQTT'] = {
            'broker': 'localhost',
            'port': '1883',
            'client_id': '',
            'username': '',
            'password': '',
            'topic_prefix': '',
            'publish_interval': '5',
            'qos': '1',
            'use_tls': 'false',
            'use_auth': 'false',
        }
        



    def setup(self) -> bool:
        """
        Configura todos os componentes do sistema.

        Returns:
            True se a configuração foi bem-sucedida
        """
        logger.info("=" * 60)
        logger.info(f"Fake dispositivo v{VERSION}" )
        logger.info("=" * 60)

        # Lendo um parâmetro/variável de ambiente configurada no Railway
        API_KEY = os.getenv("MQTT_broker", "MQTT_broker")
        logger.info(f"Broker from .env: {API_KEY}")

        
        # Configurar MQTT
        mqtt_config = {
            #'broker':       self.config_ini.get('MQTT', 'broker', fallback='localhost'),
            'broker':       os.getenv("MQTT_broker", "localhost"),
            'port':         int(os.getenv("MQTT_port", "1883")),
            'client_id':    os.getenv("MQTT_client_id", "fake_responser"),
            'topic_prefix': os.getenv("MQTT_topic_prefix", "fake_responser"),
            'qos':          int(os.getenv("MQTT_qos", "1")),
        }

        # Configurar autenticação MQTT se habilitada
        use_auth = os.getenv("MQTT_use_auth", "false").lower() == "true"

            
        if use_auth:
            mqtt_config['username'] = os.getenv("MQTT_username", '')
            mqtt_config['password'] = os.getenv("MQTT_password", '')

        mqtt_config['use_tls'] = os.getenv("MQTT_use_tls", "false").lower() == "true"


        # Inicializar gerenciador MQTT
        self.mqtt_manager = MQTTManager(**mqtt_config)


        # Conectar ao MQTT
        logger.info(f"Conectando MQTT: {mqtt_config['broker']}:{mqtt_config['port']}")
        logger.info(f"CONFIG MQTT: {self.mqtt_manager.client_id}")
        if not self.mqtt_manager.connect():
            logger.error("Falha ao conectar ao broker MQTT")
            return False

        return True


    def start(self):
        """Inicia o serviço."""

        self._running = True
        #publish_interval = self.config_ini.getint('MQTT', 'publish_interval', fallback=5)
        publish_interval = int(os.getenv("MQTT_publish_interval", "5"))
        self._last_publish_time = time.time()

        try:
            while self._running:
                current_time = time.time()

                # Publicar posição periodicamente
                if current_time - self._last_publish_time >= publish_interval:
                    self._last_publish_time = current_time

                time.sleep(0.5)

        except Exception as e:
            logger.error(f"Erro fatal no serviço: {e}", exc_info=True)
        finally:
            self.stop()

    def stop(self):
        """Para o serviço de rastreamento."""
        logger.info("Parando serviço de fake...")
        self._running = False

        # Publicar status offline
        if self.mqtt_manager:
            self.mqtt_manager.disconnect()

        logger.info("Serviço parado com sucesso")



def create_logger(level: str = 'INFO', log_dir: Optional[str] = None):
    """
    Configura o sistema de logging.

    Args:
        level: Nível de log (DEBUG, INFO, WARNING, ERROR)
        log_dir: Diretório para arquivo de log
    """
    log_level = getattr(logging, level.upper(), logging.INFO)

    # Formatar
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )

    # Handler console
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    console_handler.setFormatter(formatter)

    # Root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    root_logger.addHandler(console_handler)

    # Arquivo de log (se diretório especificado)
    if log_dir:
        try:
            os.makedirs(log_dir, exist_ok=True)
            log_file = os.path.join(log_dir, 'vehicle-tracker.log')
            file_handler = logging.FileHandler(log_file)
            file_handler.setLevel(log_level)
            file_handler.setFormatter(formatter)
            root_logger.addHandler(file_handler)
            logger.info(f"Log em arquivo: {log_file}")
        except Exception as e:
            logger.warning(f"Falha ao criar log em arquivo: {e}")


def main():
    """Ponto de entrada principal do serviço."""

    # Configurar parser de argumentos
    import argparse

    parser = argparse.ArgumentParser(
        description='Fake responser - Dispositivo Fake MQTT',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
            Exemplos:
            python -m src.tracker --config config/config.ini
        """,
    )

    parser.add_argument(
        '--config', '-c',
        help='Arquivo de configuração INI',
        default=None,
    )

    
    parser.add_argument(
        '--log-level', '-l',
        help='Nível de log (DEBUG, INFO, WARNING, ERROR)',
        default='INFO',
    )


    args = parser.parse_args()

    # Configurar logging
    create_logger(args.log_level)

    
    # Inicializar dispFake
    dispFake = FAKEResponser(args.config)

    

    # Configurar e iniciar
    if dispFake.setup():
        logger.info("Configuração concluída. Iniciando....")
        dispFake.start()
    else:
        logger.error("Falha na configuração. Verifique:")
        logger.error("  1. Broker MQTT está acessível")
        logger.error("  2. Permissões de acesso")
        sys.exit(1)



if __name__ == '__main__':
    main()
