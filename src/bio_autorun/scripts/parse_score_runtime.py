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

def parse_best_score_and_cpu_time(file_path, use_total_time=False):
    with open(file_path, "r") as f:
        content = f.read()

        # Extract best score
        score_match = re.search(r"^BEST SCORE FOUND : (-?\d+\.\d+)$", content, re.M)
        best_score = float(score_match.group(1)) if score_match else None

        if use_total_time:
            # Extract CPU time
            cpu_time_match = re.search(r"^Total wall-clock time used: (\d+\.\d+) sec", content, re.M)
            cpu_time = float(cpu_time_match.group(1)) if cpu_time_match else None
        else:
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
    parser.add_argument("--include-skipped", action="store_true", help="Include skipped data in the output")
    parser.add_argument("--print-iter-map", action="store_true", help="Print the iteration mapping")
    parser.add_argument("--print-time-map", action="store_true", help="Print the time mapping")
    parser.add_argument("--no-epsilon", action="store_true", help="Disable the use of epsilon for comparison")
    parser.add_argument("--pythia", default="", help="Path to the pythia difficulty file")
    parser.add_argument("--use-total-time", action="store_true", help="Use total time instead of tree search time")
    args = parser.parse_args()

    settings = import_settings(args.settings)
    data_names = get_data_file(settings.DATA_DIR)

    dict_score = {}
    dict_time = {}
    dict_iters = {}
    dict_difficulty = {}

    if args.pythia:
        with open(args.pythia, "r") as f:
            with open(args.pythia, "r") as f:
                csv_reader = csv.reader(f)
                for row in csv_reader:
                    data = row[0]
                    difficulty = row[1]
                    dict_difficulty[data] = difficulty


    for command_name in settings.COMMANDS.keys():
        for seed in settings.SEEDS:
            for data in data_names:
                if data in settings.SKIPPED_DATA and not args.include_skipped:
                    continue
                if settings.INCLUDED_DATA is not None and data not in settings.INCLUDED_DATA:
                    continue
                log_file = os.path.join(settings.OUTPUT_DIR, f"{data}_{command_name}_{seed}.log")
                try:
                    score, time, iters = parse_best_score_and_cpu_time(log_file, args.use_total_time)
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
                if data in settings.SKIPPED_DATA and not args.include_skipped:
                    continue
                if settings.INCLUDED_DATA is not None and data not in settings.INCLUDED_DATA:
                    continue
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
    
    if len(settings.COMMANDS) >= 2:

        # do comparison
        x = [[] for _ in range(len(settings.COMMANDS))]
        y = [[] for _ in range(len(settings.COMMANDS))]

        squared_diff = [[] for _ in range(len(settings.COMMANDS))]

        for data in data_names:
            if data in settings.SKIPPED_DATA and not args.include_skipped:
                continue
            if settings.INCLUDED_DATA is not None and data not in settings.INCLUDED_DATA:
                continue
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

            for i in range(1, len(settings.COMMANDS)):
                diff_score = datas[i][0] - datas[0][0]
                diff_time = (datas[i][1] - datas[0][1]) / 60
                # print(diff_time, datas[1][1], datas[0][1])
                x[i].append(diff_score)
                y[i].append(diff_time)
            
            best_score = -10**18
            for i in range(len(settings.COMMANDS)):
                best_score = max(best_score, datas[i][0])
            print(f"Best score for {data}: {best_score}")
            for i in range(len(settings.COMMANDS)):
                squared_diff[i].append((datas[i][0] - best_score) ** 2)
        for i in range(len(settings.COMMANDS)):
            print(f"Average squared difference for {list(settings.COMMANDS)[i]}: {sum(squared_diff[i]) / len(squared_diff[i])}")
            for j, data in enumerate(data_names):
                if data in settings.SKIPPED_DATA and not args.include_skipped:
                    continue
                if settings.INCLUDED_DATA is not None and data not in settings.INCLUDED_DATA:
                    continue
                if i > 0:
                    print(f"Score difference {list(settings.COMMANDS)[i]} {data}: {x[i][j]}, difficulty: {dict_difficulty.get(data, "")}")
        for i in range(1, len(settings.COMMANDS)):
            plt.clf()
            plt.scatter(x[i], y[i], s=3)
            plt.title(f"Compare {list(settings.COMMANDS)[i]} with {list(settings.COMMANDS)[0]}")
            plt.xlabel("Difference in Score")
            plt.ylabel("Difference in Time (minutes)")
            plt.savefig(f"{list(settings.COMMANDS)[i]}.png")
            print("==========")
            og_better = 0
            cur_better = 0
            for diff_score in x[i]:
                if args.no_epsilon:
                    if diff_score > 0:
                        cur_better += 1
                    else:
                        og_better += 1
                elif abs(diff_score) > 0.1:
                    if diff_score > 0:
                        cur_better += 1
                    else:
                        og_better += 1
            print(f"{list(settings.COMMANDS)[i]} better: {cur_better}, {list(settings.COMMANDS)[0]} better: {og_better}, ratio: {(cur_better - og_better) / len(x[i])}")
    
    if args.print_iter_map:
        assert len(settings.COMMANDS) == 1, "Iter mapping only supports one experiment"
        for command_name in settings.COMMANDS.keys():
            for data in data_names:
                if data in settings.SKIPPED_DATA and not args.include_skipped:
                    continue
                if settings.INCLUDED_DATA is not None and data not in settings.INCLUDED_DATA:
                    continue
                list_iters = []
                for seed in settings.SEEDS:
                    list_iters.append(dict_iters[(command_name, data, seed)])
                avg_iters = round(sum(list_iters) / len(list_iters))

                print(f"\"{data}\": {avg_iters},")
    
    if args.print_time_map:
        assert len(settings.COMMANDS) >= 1, "Time mapping requires at least one command"
        command_name = list(settings.COMMANDS.keys())[0]
        for data in data_names:
            if data in settings.SKIPPED_DATA and not args.include_skipped:
                continue
            if settings.INCLUDED_DATA is not None and data not in settings.INCLUDED_DATA:
                continue
            list_time = []
            for seed in settings.SEEDS:
                list_time.append(dict_time[(command_name, data, seed)])
            avg_time = round(sum(list_time) / len(list_time))

            print(f"\"{data}\": {math.ceil(avg_time / 60)},")
                

if __name__ == "__main__":
    main()
