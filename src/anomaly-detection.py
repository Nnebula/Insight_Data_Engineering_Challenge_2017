import math
import json 
from pprint import pprint

# path
input_path = '../sample_dataset/'
output_path = '../log_output/'

# Clear existing file
fout = open(output_path + 'flagged_purchases.json', 'w')
fout.close()


# Read and parse the historical dataset
fin = open(input_path + 'batch_log.json', 'r')

# read header
d = json.loads(fin.readline())
# Number of degrees in social network (at least 1).
param_D = int(d['D'])
# Number of consecutive purchases made by a user's social network (not including the user's own purchases, at least 2)
param_T = int(d['T']) 

def calculate_mean_sd(d_list):
    """ Calculates mean and standard deviation of a list.
    input: list of purchases. 
    output: mean and standard deviation of the purchases.
    """   
    d_mean = sum(d_list)/len(d_list)
    
    diff_sqr = [(val - d_mean) ** 2.0 for val in d_list]
    var = sum(diff_sqr)/len(d_list)
    d_sd = math.sqrt(var)
    
    return d_mean, d_sd

def is_anormal(amount, d_mean, d_sd):
    """ Determines whether a value is anomaly or not. i.e. > mean + 3*standard deviation
    input: mean, sd, amount
    output: true, false
    """
    return amount > (d_mean + 3*d_sd)

last_n_amounts_per_user = dict()
social_ntwk = dict()

def add_amount_to_user(user_id, amount):
    """ Recursive DFS(Depth-first search) function, adds purchase amount of a user to other users D degrees apart (D=1 friends)
    input: user_id: user made a purchase, amount, depth: D
    """
    if user_id not in last_n_amounts_per_user:
        last_n_amounts_per_user[user_id] = [amount]
    else: 
        # keep last T-1 items
        l = last_n_amounts_per_user[user_id][-param_T+1:]
        # add the current amount
        l.append(amount)
        last_n_amounts_per_user[user_id] = l 

already_added = set()
def add_friends(user_id, amount, depth):
    if depth == 0:
        return
    if user_id in already_added:
        return
    if user_id not in social_ntwk:
        return
    already_added.add(user_id)
    for friend_id in social_ntwk[user_id]:
        add_amount_to_user(friend_id, amount)
        add_friends(friend_id, amount, depth - 1)

def parse_line(line, find_anomalies):
    data = json.loads(line)
    if data['event_type'] == 'purchase':
        
        user_id = int(data['id'])
        amount = float(data['amount'])

        if find_anomalies and user_id in last_n_amounts_per_user:
            amounts = last_n_amounts_per_user[user_id]
            d_mean, d_sd = calculate_mean_sd(amounts)
            if is_anormal(amount, d_mean, d_sd):
                data['mean'] = "%0.2f" % d_mean
                data['sd'] = "%0.2f" % d_sd
                with open(output_path + 'flagged_purchases.json', 'a') as fout:
                    json.dump(data, fout)
                    fout.write("\n")

        already_added.clear()
        add_friends(user_id, amount, param_D)

                            
    elif data['event_type'] == 'befriend':
        id1 = int(data['id1'])
        id2 = int(data['id2'])
        
        if id1 not in social_ntwk:
            social_ntwk[id1] = set([id2])
        else:
            social_ntwk[id1].add(id2)
        if id2 not in social_ntwk:
            social_ntwk[id2] = set([id1])
        else:
            social_ntwk[id2].add(id1)
        
    elif data['event_type'] == 'unfriend':
        id1 = int(data['id1'])
        id2 = int(data['id2'])

        social_ntwk[id1].remove(id2)
        social_ntwk[id2].remove(id1)
    
lines = 0
for line in fin:
    lines += 1
    parse_line(line, False)

for line in open(input_path + 'stream_log.json', 'r'):
    if not line.strip():
        continue
    parse_line(line, True)
