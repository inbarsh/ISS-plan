import csv


def store_results_to_csv_file(columns, data, file_name):
    try:
        with open(file_name + ".csv", 'w') as out:
            csv_out = csv.writer(out)
            csv_out.writerow(columns)
            csv_out.writerows(data)
    except Exception as exc:
        print(exc)