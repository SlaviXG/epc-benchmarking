import json
import os.path
import subprocess
from queue import Queue
from threading import Event


from mqtt_system_governor.commander import BaseCommander, init_commander

# !!!
# Make sure that jsonify = True and save_feedback = False inside mqtt_system_governor/config.ini
# !!!

RASPBERRY_CLIENT_ID = "client1"
MIN_TEMPERATURE_DIFFERENCE = 0.3
LOGGER_STARTING_COMMAND = "sudo fnirsi_usb_power_data_logger/fnirsi_logger.py"
COMMAND_FEEDBACK_FILE = "command_feedback.txt"
LOGGER_OUTPUT_FILE = "data_logger.txt"
FEEDBACK_QUEUE = Queue()
START_OPERATOR_COMMAND = "python3 mqtt_system_governor/operator.py --config=mqtt_system_governor/config.ini"


def save_feedback_to_file(feedback: str):
    with open(COMMAND_FEEDBACK_FILE, 'a') as f:
        f.write(feedback + '\n')


class StressRaspberry:
    class Commander(BaseCommander):
        def on_message(self, client, userdata, msg):
            feedback = msg.payload.decode()
            if self._jsonify:
                try:
                    feedback = json.loads(feedback)
                    if feedback['client_id'] == RASPBERRY_CLIENT_ID:
                        save_feedback_to_file(feedback)
                        FEEDBACK_QUEUE.put(feedback)
                        print(f"Feedback received and saved (command: {feedback['command']})")
                        print(f"{json.dumps(feedback, indent=2)}")
                except json.JSONDecodeError as e:
                    print(f"(!) Please make sure that feedback was sent in a JSON format\n{feedback}")
            else:
                print(f"(!) Please set the `jsonify` option to True inside mqtt_system_governor/config.ini")

    def __init__(self):
        self.commander = init_commander(os.path.join('mqtt_system_governor', 'config.ini'))
        self.command_queue = Queue()
        self.fill_command_queue()
        self.operator_process = None
        self._power_data_logger_process = None
        self._save_logger_output = Event()
        self._awaiting_for_feedback = Event()

    def fill_command_queue(self):
        # for processor_utilization in range(10, 101, 10):
        #     for frequency in range(600000, 1800001, 100000):
        #         self.command_queue.put(f"sudo cpufreq-set -r -f {frequency}")
        #         self.command_queue.put(self.form_cpu_stress_command(processor_utilization))
        self.command_queue.put(f"ls")
        self.command_queue.put(f"ls -a")
        self.command_queue.put(f"ls -la")

    def start_power_data_logger(self):
        self._power_data_logger_process = subprocess.Popen(LOGGER_STARTING_COMMAND,
                                                           stdout=subprocess.PIPE,
                                                           stderr=subprocess.PIPE,
                                                           text=True, shell=True)

    @staticmethod
    def form_cpu_stress_command(utilization, cores=0, time=60):
        return f"stress-ng --cpu {cores} --cpu-load {utilization} --timeout {time}s --metrics-brief"

    @staticmethod
    def parse_logger_output_line(line):
        keys = ["timestamp", "sample_in_packet", "voltage_V", "current_A", "dp_V", "dn_V", "temp_C_ema", "energy_Ws",
                "capacity_As"]
        values = line.split()
        values = [float(value) for value in values]
        result = dict(zip(keys, values))
        return result

    def start_operator(self):
        with open(os.devnull, 'w') as devnull:
            self.operator_process = subprocess.Popen(START_OPERATOR_COMMAND, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, shell=True)
            return self.operator_process

    def run(self):
        operator_process = self.start_operator()
        while True:
            operator_output = operator_process.stdout.readline()
            if operator_output == '' and operator_process.poll() is not None:
                break
            if operator_output:
                print(operator_output.strip())
                if 'Registered clients:' in operator_output:
                    break

        self.commander.connect()

        # Set additional metrics
        first_line_passed = False
        initial_temperature = None
        current_command = None

        print("Starting benchmarking...")

        with open(LOGGER_OUTPUT_FILE, 'w') as f:
            self.start_power_data_logger()
            while True:
                # Read power data logger output
                logger_output = self._power_data_logger_process.stdout.readline()
                if first_line_passed and logger_output == '' and self._power_data_logger_process.poll() is not None:
                    print("The logger process was stopped.")
                    break
                if logger_output.strip():
                    if self._save_logger_output.is_set():
                        f.write(logger_output)
                        f.flush()
                else:
                    continue

                try:
                    # Make a dictionary out of the output
                    logger_output = self.parse_logger_output_line(logger_output)
                except Exception as e:
                    # Occurs when trying to convert string to float, making sure to pass the first logger output line
                    first_line_passed = True
                    print(f"First output line passed: ({e})")
                    continue

                # Set initial temperature if it wasn't set
                if initial_temperature is None:
                    initial_temperature = logger_output['temp_C_ema']
                    print(f"Initial temperature: {initial_temperature} C")

                # Check if there is a need to cool down the device
                if logger_output['temp_C_ema'] - initial_temperature > MIN_TEMPERATURE_DIFFERENCE:
                    self._save_logger_output.clear()
                else:
                    if not self._awaiting_for_feedback.is_set():
                        # Start saving output
                        self._save_logger_output.set()

                        # Get the command from the queue
                        current_command = self.command_queue.get()
                        if current_command is None:
                            break

                        # Send the command
                        self.commander.send_command(RASPBERRY_CLIENT_ID, current_command)
                        self._awaiting_for_feedback.set()
                    else:
                        feedback = FEEDBACK_QUEUE.get()
                        if feedback is not None:
                            # Check if the command matches with the last one sent:
                            if feedback['client_id'] == RASPBERRY_CLIENT_ID and feedback['command'] == current_command:
                                self._awaiting_for_feedback.clear()
                                self._save_logger_output.clear()
                        else:
                            pass



