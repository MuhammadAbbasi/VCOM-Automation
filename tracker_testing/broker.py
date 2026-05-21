import sys
import asyncio

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

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
    for attempt in range(5):
        broker = Broker(config)
        try:
            await broker.start()
            print("[BROKER] MQTT Broker started on 0.0.0.0:1883", flush=True)
            while True:
                await asyncio.sleep(60)
        except (asyncio.CancelledError, KeyboardInterrupt):
            break
        except OSError as e:
            # Port still in TIME_WAIT from previous run — retry
            print(f"[BROKER] Port busy (attempt {attempt + 1}/5): {e}", flush=True)
            if attempt < 4:
                await asyncio.sleep(5)
                continue
            print("[BROKER] Could not bind after 5 attempts. Exiting.", flush=True)
            break
        except Exception as e:
            print(f"[BROKER] Error: {e}", flush=True)
            break
        finally:
            try:
                await broker.shutdown()
            except Exception:
                pass
        break

    print("[BROKER] Broker stopped.", flush=True)

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("[BROKER] Broker stopped.", flush=True)
