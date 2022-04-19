# remove old result
rm -f ./logs/*.log
rm -f ./logs/*.storage
rm -f ./logs/*.state_machine

sudo ../venv/bin/python3.6 run_cluster.py