# Python scripts for processing USPTO inventor and patent data

The following collection of scripts performs pre- and
post-processing on patent data as part of the patent
inventor disambiguation process.

Follow development, subscribe to
[RSS
feed](https://github.com/funginstitute/patentprocessor/commits/master.atom).

## Processing patents

There are two ways to get started:

* Run `preprocess.sh <path-to-config-file> <number-of-threads>`

  See `process.cfg` for an example of a configuration file. The options in the
  `[process]` section will be used to determine which data is parsed, which
  steps are run, and where the data will be located after the process finishes.
  This process requires [`IPython`](http://ipython.org/install.html) to be
  installed.

* run `parse.py` directly to customize which directories are processed and
  which regex is used to process the files. Run `parse.py -h` to see the
  relevant command-line options. Follow with `clean.py` then
  `consolidate.py` to obtain a full set of tables.

To run the `clean.py` script, the
[location](https://s3.amazonaws.com/fungpatdownloads/geolocation_data.7z)
table must exist in the `patentprocessor/lib` directory. File requires [7zip](http://www.7-zip.org/) to unpack.

## Configuring the Preprocessing Environment

The python-based preprocessor is tested on Ubuntu 12.04 and MacOSX 10.6.  Any
flavor of Unix with the following installed should work, though it is possible
to get the toolchain running on Windows.

If you have [`pip`](http://www.pip-installer.org/en/latest/index.html)
installed, you can simplify the installation process by just running `sudo pip
install -r requirements.txt` from within the patentprocessor directory.

Please [file an issue](https://github.com/funginstitute/patentprocessor/issues) if you find another dependency.

### Ubuntu

```
sudo apt-get update
sudo apt-get install python-dev
sudo apt-get install python-setuptools
sudo easy_install -U distribute
sudo apt-get install -y python-Levenshtein make libmysqlclient-dev python-mysqldb python-pip python-zmq python-numpy gfortran libopenblas-dev liblapack-dev g++ sqlite3 libsqlite3-dev python-sqlite
sudo pip install -r requirements.txt
```

In order to properly configure the preprocessing environment, the end user must
manually perform the following:

* Download the relevant XML files which need to be processed. These can be
  placed in any directory, but `parse.py` assumes the current directory `.`.
  So far, the parser can handle schemas 4.2, 4.3 and 4.4 for Patent XML files,
  which can be found going back to 2005
  [here](http://www.google.com/googlebooks/uspto-patents-grants-text.html).

## Contributing to the Patent Processor Project

Contributions are welcome, for source code development, testing (including
validation and verification), uses cases, etc. We are targeting general
PEP-compliance, so even an issue noting where we could do better is
appreciated.

### Contributing code

Pull requests are especially welcome. Here are a few pointers which will make everything easier:

* Small, tightly constrained commits.
* New files should be in their own commit, and committed before they are used in subsequent commits.
* Commits should tell a story in a logical sequence. It should be possible to understand the gist
  of the development just from reading the commits (hard, but worthwhile goal).
* The ideal commit:
    * Unit (or similar) test for a single functionality.
    * Implementation to pass the unit test.
    * Documentation (the "why") of the function/method in the appropriate location (platform dependent).
    * 0 or 1 use of the new functionality in production.
    * Further uses of functionality should go in future commits.
* Formatting updates, code cleanup and renaming should go into independent commits.
* Submit only code which is covered by *working* unit tests.
* Testing scripts, including unit tests, integration tests and functional tests go in the `test` directory.
* Code which does work goes in the lib directory.
* Code which provides a workflow (i.e., processing patents or building necessary
  infrastructure) goes in the top level directory. In the future, much of this code may
  be put into a `bin` directory.
* Test code should follow the pattern `test/test_libfile.py`. This pattern may change in
  the future, whence this documentation will change at that time.

**You must rebase before issuing a pull request: `git pull --rebase <upstream> master`**.

### Coding style

Start with [PEP8](http://www.python.org/dev/peps/pep-0008/). A very
large number of extremely intelligent software engineers working at the
wealthiest corporations on the planet have more or less agreed on a
standard set of conventions allowing J. Random Coder (that's you and I)
to read and write Python like the Big Boys and Girls (PyLadies!).
It *highly* unlikely we can improve on these guidelines.

That said, rules are rules and exists to broken once in a while.
So, optimize for readability.  Specifically:

* Use vowels, not secret shorthand 1337 cmptr cd fr nmng vrbls.
* Line length to 80 characters, no more.

### Running Tests

Before committing changes or submitting a pull request, please make sure that the code passes
all of our tests. There are two sets of tests:

* Integration tests: these test the end-to-end status of the preprocessor. From
  within the `integration/` directory, run the script
  `run_integration_tests.sh`. If you do not see any diff output, then the test
  has passed.
* Unit tests: these test individual components of the preprocessor. From within
  the `test/` directory, run the script `patenttest.sh`. The output will let
  you know if any tests have failed.

### Configuring for testing

Currently, testing requires having the environment configured as above,
and having some of the processing results. That is, testing the
"cleaning" phase requires having files from the parse phase.
