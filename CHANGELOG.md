# CHANGE LOG

## 0.3.2

- [enhancement] updated the `reassign` command to rebalance partitions accross brokers whith the minimum number of partition moves
- [bug] [todo] The migrate function can sometimes fail, because i try to move partitions from the biggest unbalanced node to the lowest unbalanced node, and sometimes there is not enough difference. Instead, i need to move remaining partitions that are already there on the other unbalanced nodes. Nevertheless, if that happens, simply restart the script until the combination fits.

## 0.3.1

- [feature] add `verify-reassign` command

## 0.3.0
- [feature] add `verify-replicas` command
- [bug-fix] use a pool of Zookeeper connections and handle Zookeeper errors correctly

## 0.2.0
- [feature] add `cluster-restart` command
