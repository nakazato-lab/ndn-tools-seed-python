import logging
import sys
from ndn.app import NDNApp
from ndn.types import InterestNack, InterestTimeout, InterestCanceled, ValidationFailure
from ndn.encoding import Name
from lib.ndn_utils import get_data

# 処理を要求するクライアント

logging.basicConfig(format='[{asctime}]{levelname}:{message}',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.INFO,
                    style='{')

app = NDNApp()

async def main():
    if len(sys.argv) < 2:
        print("Usage: python consumer.py <interest-name>")
        app.shutdown()
        return

    interest_name = sys.argv[1]

    try:
        content = await get_data(app, interest_name)
        print(bytes(content).decode('utf-8', errors='ignore'))
    except InterestNack as e:
        print(f'Nacked with reason={e.reason}')
    except InterestTimeout:
        print('Timeout')
    except InterestCanceled:
        print('Canceled')
    except ValidationFailure:
        print('Data failed to validate')
    finally:
        app.shutdown()

if __name__ == '__main__':
    app.run_forever(after_start=main())
