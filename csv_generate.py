import csv


def save_csv(list_sql_row, csv_file, delimiter):
    with open(csv_file, 'w', encoding='UTF8', newline='', ) as f:
        writer = csv.writer(f, delimiter=delimiter, quoting=csv.QUOTE_ALL)
        for row in list_sql_row:
            writer.writerow(row)
            f.flush()
