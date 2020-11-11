.PHONY: test
test:
	rm -f .coverage
	nosetests --rednose --exe --cover-package=s3po --with-coverage --cover-branches --logging-clear-handlers -v

test3:
	rm -f .coverage
	python3 -m nose --rednose --exe --cover-package=s3po --with-coverage --cover-branches --logging-clear-handlers -v

clean:
	# Remove the build
	rm -rf build dist
	# And all of our pyc files
	find . -name '*.pyc' | xargs -n 100 rm
	# And lastly, .coverage files
	find . -name .coverage | xargs rm
