import argparse
import importlib.util
import os
import re
import csv
import math
from matplotlib import pyplot as plt

def import_settings(settings_path):
    spec = importlib.util.spec_from_file_location("settings", settings_path)
    settings = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(settings)
    return settings

def get_data_file(data_dir):
    data = []
    for name in os.listdir(data_dir):
        if name.endswith(".phy"):
            data.append(name)
    return data

def parse_best_score_and_cpu_time(file_path):
    with open(file_path, "r") as f:
        content = f.read()

        # Extract best score
        score_match = re.search(r"^BEST SCORE FOUND : (-?\d+\.\d+)$", content, re.M)
        best_score = float(score_match.group(1)) if score_match else None

        # Extract CPU time
        cpu_time_match = re.search(r"^CPU time used for tree search: (\d+\.\d+) sec", content, re.M)
        cpu_time = float(cpu_time_match.group(1)) if cpu_time_match else None

        # extract number of iters
        iters_match = re.search(r"^TREE SEARCH COMPLETED AFTER (\d+) ITERATIONS", content, re.M)
        iters = int(iters_match.group(1)) if iters_match else None

        if best_score is not None and cpu_time is not None and iters is not None:
            return best_score, cpu_time, iters

def main():
    parser = argparse.ArgumentParser(description="Load a settings.py file.")
    parser.add_argument(
        "-s",
        "--settings",
        default="settings.py",
        help="Path to the settings.py file to import"
    )
    parser.add_argument("--print-iter-map", action="store_true", help="Print the iteration mapping")
    args = parser.parse_args()

    settings = import_settings(args.settings)
    data_names = get_data_file(settings.DATA_DIR)

    dict_score = {}
    dict_time = {}
    dict_iters = {}

    for command_name in settings.COMMANDS.keys():
        for seed in settings.SEEDS:
            for data in data_names:
                log_file = os.path.join(settings.OUTPUT_DIR, f"{data}_{command_name}_{seed}.log")
                try:
                    score, time, iters = parse_best_score_and_cpu_time(log_file)
                    dict_score[(command_name, data, seed)] = score
                    dict_time[(command_name, data, seed)] = time
                    dict_iters[(command_name, data, seed)] = iters
                except Exception as e:
                    print(f"Error parsing {log_file}", e)

    for command_name in settings.COMMANDS.keys():
        with open(f"{command_name}.csv", "w", newline="") as csvfile:
            writer = csv.writer(csvfile)
            labels = ["Dataset", "Model", "Average Score", "Average Time", "Average Iters"]
            writer.writerow(labels)
            sum_time = 0
            for data in data_names:
                list_score = []
                list_time = []
                list_iters = []
                for seed in settings.SEEDS:
                    list_score.append(dict_score[(command_name, data, seed)])
                    list_time.append(dict_time[(command_name, data, seed)])
                    list_iters.append(dict_iters[(command_name, data, seed)])
                avg_score = sum(list_score) / len(list_score)
                avg_time = sum(list_time) / len(list_time)
                avg_iters = round(sum(list_iters) / len(list_iters))
                row = [data, settings.MODELS[data], avg_score, avg_time, avg_iters]
                writer.writerow(row)
                sum_time += avg_time
            print(f"Total time for {command_name}: {sum_time}")
    
    if len(settings.COMMANDS) == 2:
        # do comparison
        x = []
        y = []

        for data in data_names:
            datas = []
            for index, command_name in enumerate(settings.COMMANDS.keys()):
                list_score = []
                list_time = []
                list_iters = []
                for seed in settings.SEEDS:
                    list_score.append(dict_score[(command_name, data, seed)])
                    list_time.append(dict_time[(command_name, data, seed)])
                    list_iters.append(dict_iters[(command_name, data, seed)])
                avg_score = sum(list_score) / len(list_score)
                avg_time = sum(list_time) / len(list_time)
                avg_iters = round(sum(list_iters) / len(list_iters))
                datas.append((avg_score, avg_time))
            assert len(datas) == 2
            diff_score = datas[1][0] - datas[0][0]
            diff_time = (datas[1][1] - datas[0][1]) / 60
            # print(diff_time, datas[1][1], datas[0][1])
            x.append(diff_score)
            y.append(diff_time)
        plt.scatter(x, y, s=3)
        plt.savefig("test.png")
        
    
    if args.print_iter_map:
        assert len(settings.COMMANDS) == 1, "Iter mapping only supports one experiment"
        for command_name in settings.COMMANDS.keys():
            for data in data_names:
                list_iters = []
                for seed in settings.SEEDS:
                    list_iters.append(dict_iters[(command_name, data, seed)])
                avg_iters = round(sum(list_iters) / len(list_iters))

                print(f"\"{data}\": {avg_iters},")
                

if __name__ == "__main__":
    main()
