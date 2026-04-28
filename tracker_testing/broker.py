import asyncio
import logging
from amqtt.broker import Broker

# Configure logging
logging.basicConfig(level=logging.INFO)

config = {
    'listeners': {
        'default': {
            'type': 'tcp',
            'bind': '0.0.0.0:1883',
        }
    },
    'sys_interval': 10,
    'auth': {
        'allow-anonymous': True,
    }
}

async def main():
    broker = Broker(config)
    try:
        await broker.start()
        print("[BROKER] MQTT Broker started on 0.0.0.0:1883")
        while True:
            await asyncio.sleep(3600)
    except Exception as e:
        print(f"[BROKER] Failed to start broker: {e}")

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("[BROKER] Broker stopped.")
