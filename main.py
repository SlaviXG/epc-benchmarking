from benchmark.stress_raspberry import StressRaspberry

if __name__ == '__main__':
    stress_raspberry = StressRaspberry()
    try:
        stress_raspberry.run()
    except KeyboardInterrupt:
        stress_raspberry.terminate_processes()
        print("\nBenchmarking terminated by user.")
