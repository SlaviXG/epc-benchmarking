
## Getting Started

## Requirements
- **Linux**
- **Python**
- **MQTT broker**
- **libusb**

### Retrieve and update the dependencies:
```bash
git submodule update --init --recursive
```

### Configure the parameters
```bash
nano benchmark/stress_raspberry.py
```

Make sure to set/check that `jsonify=True` and `save_feedback=False` inside `mqtt_system_governor/config.ini`.

###  Running the test:
```bash
sudo python3 main.py
```