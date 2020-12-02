# restful-modbus-api (Restful MODBUS API)

## Template File

```bahs
$ restful-modubs-api -t sample_template.yml
```

```json
{
    "id":"test-3",
    "use": true,
    "description": "Test",
    "comm": {
        "type": "tcp",
        "setting": {
        "host": "localhost",
        "port": 502
        }
    },
    "seconds": 3,
    "code": "def main():\n    a = read_holding_registers('-a 40001 -c 4')\n    b = read_holding_registers('-a 40017 -c 2')\n    b = read_holding_registers('-a 40019 -c 2')\n    return a + b\n",
    "template": [
        {
        "key": "data01",
        "note": "String",
        "type": "B64_STRING"
        },
        {
        "key": "data02",
        "note": "unsigned integer value",
        "type": "B16_UINT"
        },
        {
        "key": "data03",
        "note": "integer value",
        "type": "B16_INT"
        }
    ]
}

```
