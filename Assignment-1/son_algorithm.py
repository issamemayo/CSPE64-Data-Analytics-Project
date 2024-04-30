from pyspark import SparkContext
import time
from itertools import combinations
 
startTime = time.time()

# Set the support threshold and input/output file paths
supportThreshold = 300
inputFile = './DataSet/big.csv'
outputFile = "./Outputs/son_output.txt"

# Initialize SparkContext
sc = SparkContext("local[*]", "SON Algorithm")
sc.setLogLevel("ERROR")

# Read input data and preprocess
textData = sc.textFile(inputFile)
firstRow = textData.first()
filteredTextData = textData.filter(lambda x: x != firstRow)
basicRDD = filteredTextData.map(lambda x: x.split(","))

candidate_list = []
frequent_item_list = []


def generate_candidates(basket, support_val, prev_candidates, set_size):
    """`
    Generate candidate itemsets for Apriori pass 1.

    Args:       
        basket: List of baskets in the partition.
        support_val: Support threshold for the baskets.
        prev_candidates: List of previous candidate itemsets.
        set_size: Size of the candidate itemsets to be generated.

    Returns:
        List of new candidate itemsets.
    """
    new_candidates = []
    general_count = {}
    if len(prev_candidates) == 0:
        return []
    new_prospective_candidates = []
    for item in combinations(prev_candidates, 2):
        new_candidate = set(item[0]).union(set(item[1]))
        if len(new_candidate) == set_size + 1:
            new_prospective_candidates.append(tuple(sorted(new_candidate)))

    new_prospective_candidates = set(new_prospective_candidates)

    for p in new_prospective_candidates:
        for x in basket:
            if set(p).issubset(x[1]):
                general_count.setdefault(p, 0)
                general_count[p] += 1

    for x in general_count:
        if general_count[x] >= support_val:
            new_candidates.append(x)
    return new_candidates



def apriori_pass1(iterator):
    """
    Apriori pass 1: Generate candidate itemsets.

    Args:
        iterator: Iterator over baskets in a partition.

    Returns:
        List of candidate itemsets for the partition.
    """
    basket = [x for x in iterator]
    basket_length = len(basket)
    support_val = basket_length * per_unit_support_val
    single_candidate_count = {}
    single_candidates = []
    final_candidates = []
    for x in basket:
        for item in x[1]:
            single_candidate_count.setdefault(item, 0)
            single_candidate_count[item] += 1
    for x in single_candidate_count:
        if single_candidate_count[x] >= support_val:
            single_candidates.append(tuple([x]))
    final_candidates += single_candidates
    previous_candidates = single_candidates
    set_size = 1

    while True:
        new_candidates = generate_candidates(basket, support_val, previous_candidates, set_size)
        final_candidates += new_candidates
        previous_candidates = new_candidates
        set_size += 1
        if len(previous_candidates) == 0:
            break
    return final_candidates


def apriori_pass2(iterator):
    """
    Apriori pass 2: Generate frequent itemsets.

    Args:
        iterator: Iterator over baskets in a partition.

    Returns:
        Yield frequent itemsets along with their counts.
    """
    basket = [x for x in iterator]
    candidates = candidate_list_broadcast.value
    candidate_count = {}
    for a, b in basket:
        for k, v in candidates:
            for item in v:
                if set(item).issubset(set(b)):
                    candidate_count.setdefault(item, 0)
                    candidate_count[item] += 1

    for item in sorted(candidate_count.keys()):
        yield (item, candidate_count[item])

# Son Algorithm Execution

# Map baskets to key-value pairs and reduce to unique baskets
basket_rdd = basicRDD.map(lambda x: (str(x[0]), {str(x[1])})).reduceByKey(lambda x, y: x.union(y))
# Count the number of baskets 
basket_count = basket_rdd.count()
# Calculate the support threshold for each basket
per_unit_support_val = (1.0 * float(supportThreshold)) / basket_count
# Generate the candidate list in Apriori pass 1
temp_candidates_list = basket_rdd.mapPartitions(apriori_pass1)
# Remove duplicates from the candidate list
candidate_list = temp_candidates_list.distinct().groupBy(len).collect()

candidate_list_broadcast = sc.broadcast(candidate_list)

# Generate frequent itemsets in Apriori pass 2
pass2_candidate_list_rdd = basket_rdd.mapPartitions(apriori_pass2).reduceByKey(lambda x, y: x + y)
frequent_item_list = pass2_candidate_list_rdd.filter(lambda x: x[1] >= supportThreshold)
frequent_item_list = frequent_item_list.map(lambda x: x[0]).groupBy(len).collect()

# Write results to output file
with open(outputFile, 'w') as file:
    # Write the number of baskets and support threshold
    file.write('Number of Baskets: ' + str(basket_count) + '\n')
    file.write('Support Threshold: ' + str(supportThreshold) + '\n\n')

    # Write candidate itemsets to the file
    file.write('Candidate Itemsets: \n')
    for a, b in sorted(candidate_list):
        candidates = [x for x in b]
        candidates = sorted(candidates)
        line = ",".join([str(tuple(items)) for items in candidates]).replace(r',)', ')')
        file.write(line + '\n\n')

    # Write frequent itemsets to the file
    file.write('Frequent Itemsets: \n')
    for a, b in sorted(frequent_item_list):
        frequent_item_sets = [x for x in b]
        frequent_item_sets = sorted(frequent_item_sets)
        line = ",".join([str(tuple(items)) for items in frequent_item_sets]).replace(r',)', ')')
        file.write(line + '\n\n')

# Calculate execution time
endTime = time.time()
print("SON Algorithm Execution Completed!")
print("Duration: ", endTime - startTime)

#Time complexity - O((m/n) * r) - m is frequent subsets, n number of transactions, r is the number of chunks