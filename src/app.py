#!/usr/bin/env python3
"""
Ponto de entrada simplificado.

Uso:
  python run.py --config config/config.ini
  python run.py --port /dev/ttyUSB0 --broker mqtt.broker.com
  python run.py --test
  python run.py --simulate
  python run.py --detect-port
"""

import sys
import os

# Adicionar diretório raiz ao path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fakeService import main

if __name__ == '__main__':
    main()
