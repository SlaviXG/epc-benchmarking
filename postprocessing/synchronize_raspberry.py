import os
import re
import pandas as pd

from mqtt_system_governor.json_feedback import parse_feedback


# Function to extract metrics from the output string
def extract_stress_ng_metrics_from_output(output):
    lines = output.split('\n')
    for line in lines:
        if 'cpu' in line and 'stress-ng: metrc:' in line:
            parts = line.split()
            bogo_ops = int(parts[4])
            real_time = float(parts[5])
            usr_time = float(parts[6])
            sys_time = float(parts[7])
            bogo_ops_per_sec_real = float(parts[8])
            bogo_ops_per_sec_usr_sys = float(parts[9])
            return [bogo_ops, real_time, usr_time, sys_time, bogo_ops_per_sec_real, bogo_ops_per_sec_usr_sys]
    return [None] * 6


# To extract cpu load
def extract_cpu_load(command):
    match = re.search(r'--cpu-load (\d+)', command)
    if match:
        return int(match.group(1))
    return None


# To form a command data frame
def form_command_df(command_feedback_file: os.path):
    command_df = pd.DataFrame(parse_feedback(command_feedback_file))

    # Extract frequencies
    command_df['frequency'] = command_df['command'].apply(lambda x: int(x.split()[-1]) if 'cpufreq-set' in x else None)

    # Shift frequencies to the next row
    command_df['frequency'] = command_df['frequency'].shift()

    # Filter out rows that contain cpufreq-set in command
    command_df = command_df[~command_df['command'].str.contains('cpufreq-set')]

    # Move 'error' content to the 'output' column
    command_df['output'] = command_df['error']

    # Extracting metrics from the output
    command_df[['bogo_ops', 'real_time', 'usr_time', 'sys_time', 'bogo_ops_per_sec_real', 'bogo_ops_per_sec_usr_sys']] = \
        command_df['output'].apply(lambda x: pd.Series(extract_stress_ng_metrics_from_output(x)))

    # Extract cpu-load from the command
    command_df['cpu_load'] = command_df['command'].apply(extract_cpu_load)

    # Remove redundant columns
    command_df = command_df.drop('error', axis=1)
    command_df = command_df.drop('client_id', axis=1)
    command_df = command_df.drop('output', axis=1)
    command_df = command_df.drop('command', axis=1)

    # Reorder columns
    column_order = ['frequency', 'cpu_load', 'start_time', 'end_time', 'bogo_ops', 'real_time', 'usr_time', 'sys_time',
                    'bogo_ops_per_sec_real', 'bogo_ops_per_sec_usr_sys']
    command_df = command_df[column_order]

    # Ensure proper data types
    command_df['start_time'] = command_df['start_time'].astype(float)
    command_df['end_time'] = command_df['end_time'].astype(float)
    command_df['frequency'] = command_df['frequency'].astype(float)

    return command_df


# Forms a logger data frame
def form_logger_df(power_data_logger_file: os.path):
    logger_df = pd.read_csv(power_data_logger_file, sep='\s+')
    logger_df = logger_df[['timestamp', 'voltage_V', 'current_A', 'temp_C_ema']]

    # Ensure proper data types
    logger_df['timestamp'] = logger_df['timestamp'].astype(float)
    logger_df['voltage_V'] = logger_df['voltage_V'].astype(float)
    logger_df['current_A'] = logger_df['current_A'].astype(float)
    logger_df['temp_C_ema'] = logger_df['temp_C_ema'].astype(float)

    return logger_df


# Merge logger and feedback data frames
def merge_command_and_logger_dfs(command_df, logger_df):
    # Initialize lists to store mean values
    mean_voltage_V = []
    mean_current_A = []
    mean_temp_C_ema = []

    # Iterate over each row in the command DataFrame
    for _, row in command_df.iterrows():
        start_time = row['start_time']
        end_time = row['end_time']

        # Filter the logger data to the relevant time period
        filtered_logger_df = logger_df[(logger_df['timestamp'] >= start_time) & (logger_df['timestamp'] <= end_time)]

        # Calculate the mean values for the filtered logger data
        mean_voltage_V.append(filtered_logger_df['voltage_V'].mean())
        mean_current_A.append(filtered_logger_df['current_A'].mean())
        mean_temp_C_ema.append(filtered_logger_df['temp_C_ema'].mean())

    # Add the mean values to the command DataFrame
    command_df['mean_voltage_V'] = mean_voltage_V
    command_df['mean_current_A'] = mean_current_A
    command_df['mean_temp_C_ema'] = mean_temp_C_ema

    return command_df


def synchronize_output_data(power_data_logger_file: os.path, command_feedback_file: os.path):
    command_df = form_command_df(command_feedback_file)
    logger_df = form_logger_df(power_data_logger_file)
    df = merge_command_and_logger_dfs(command_df, logger_df)
    print(df)
    df.to_csv('df.csv', index=False)


if __name__ == '__main__':
    synchronize_output_data(os.path.join('..', 'data_logger.txt'), os.path.join('..', 'command_feedback.txt'))
