# newspaper-bulk

A command-line script to apply [Newspaper3k](https://pypi.python.org/pypi/newspaper3k/0.1.5)'s article extraction feature in bulk, made with large lists (>1K) of URLs in mind. The script is made to be as forgiving as possible, accepting any and all URLs insofar as they are in the first column of a .csv, .xlsx, .xls, or .csv. file. It uses Python's standard `threading` library to divvy up the work (default is for 100 threads; see [Shane Lynn's tutorial](https://www.shanelynn.ie/using-python-threading-for-multiple-results-queue/)). In addition to the requirements, make sure you have `nltk`'s `punkt` package installed (via `nlkt.download()` in interactive Python) for Newspaper3k's `article.nlp()` to work properly.

# usage 

```
usage: newspaperbulk.py [-h] [-t THREADS] [-r] [-u] [-m MAX_RETRIES]
                        [-b BACKOFF]
                        filepath
                        
positional arguments:
  filepath              Enter the path of the .csv, .txt, .xlsx, or .xls file
                        containing the URLs. If the file is not in your
                        current directory, you must enter the absolute path.

optional arguments:
  -h, --help            show this help message and exit
  -t THREADS, --threads THREADS
                        Number of threads to launch (default 100).
  -r, --redirects       Select to allow redirects.
  -u, --unverified      Select to allow unverified SSL certificates.
  -m MAX_RETRIES, --max_retries MAX_RETRIES
                        Set the max number of retries (default 0 to fail on
                        first retry).
  -b BACKOFF, --backoff BACKOFF
                        Set the backoff factor (default 0).
```