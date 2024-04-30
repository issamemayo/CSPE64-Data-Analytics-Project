import csv

basket_dict = {}

csv_file = './DataSet/big.csv'  

f = open(csv_file, 'r').readlines()[1:]
main_dict = {}
for line in f:
    items = line.strip().split(",")
    if int(items[0]) not in main_dict.keys():
        main_dict[int(items[0])] = set()
        main_dict[int(items[0])].add(int(items[1]))
    else:
        main_dict[int(items[0])].add(int(items[1]))
baskets = []
for k in main_dict.keys():
    baskets.append(sorted(main_dict[k]))

output_file = './DataSet/transformed_dataset.csv'
with open(output_file, mode='w') as file:
    writer = csv.writer(file)
    for basket in baskets:
        writer.writerow(basket)
