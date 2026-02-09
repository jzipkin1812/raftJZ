USERPORT ?= 8000
COORDPORT ?= 8001
SHARDPORT ?= 8004

INPUTFILE ?= input.txt
OUTPUTFILE ?= output.txt

run_project: 
	python3 user.py --callback $(USERPORT) --coordinator $(COORDPORT) --shard $(SHARDPORT) < $(INPUTFILE) > $(OUTPUTFILE)
	./kill.sh

run_terminal: 
	python3 user.py --callback $(USERPORT) --coordinator $(COORDPORT) --shard $(SHARDPORT)
	./kill.sh
