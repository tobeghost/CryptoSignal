import os
import json
import ccxt

def getAll():
    """
    Parses the environment configuration to create the config objects.
    Initializes the configuration class
    """
    config = dict()

    if os.path.isfile('config.json'):
        with open('config.json', 'r') as f:
            config = json.load(f)

    for exchange in config['exchanges']:
        if exchange['name'] not in ccxt.exchanges:
            exchange['enabled'] = False

    return config
