# aiorefoss

Asynchronous Python library for Refoss smart devices, designed for Home Assistant integration.

## Features

- Async WebSocket connection
- Device authentication support for new firmware (EM16P v3.1.11+)
- Automatic auth challenge handling
- Real-time status updates
- Device configuration management
- Cross-platform JSON support (orjson with stdlib fallback)

## Installation

```bash
pip install aiorefoss
```

With faster JSON support:
```bash
pip install aiorefoss[fastjson]
```

## Quick Start

```python
import asyncio
from aiorefoss.rpc_device.device import RpcDevice
from aiorefoss.common import process_ip_or_options

async def main():
    options = await process_ip_or_options("192.168.1.100")
    device = await RpcDevice.create(None, options)
    await device.initialize()
    print(f"Device model: {device.model}")
    print(f"Firmware version: {device.firmware_version}")
    await device.shutdown()

asyncio.run(main())
```

## Requirements

- Python 3.12+
- aiohttp
- yarl
- orjson (optional, for better performance)

## License

MIT

## Support

- Email: support@refoss.net
- Issues: https://github.com/ashionky/aiorefoss/issues
