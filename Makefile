run:
	python2 couch2sqlite.py twitter
	python2 couch2sqlite.py sample

user:
	python2 couch2sqlite.py twitter

sample:
	python2 couch2sqlite.py sample

.PHONY : clean

clean:
