README
======

#### Installation:

```
sudo apt-get install -y git
sudo apt-get install -y redis-server
sudo apt-get install -y python-pip
sudo apt-get install -y python-zmq
sudo apt-get install -y python-mysqldb
sudo apt-get install -y python-Levenshtein
```

After git clone:

```
sudo pip install -r requirements.txt
```

```
git fetch upstream
git merge upstream\sqlalchemy
```

#### Some MySQL recipes specific to AWS:

Export files into CSV

```
mysql -u [user] -p [passwd] --database=[db] --host=[host] --batch -e "select * from [table] limit 10" | sed 's/\t/","/g;s/^/"/;s/$/"/;s/\n//g' > [table].csv
```

Allow local file reading (local-infile must be 1 for security purposes)

```
mysql -u [user] -p --local-infile=1 -h [db] [tbl]
```

```
sudo apt-get install -y p7zip-full

sudo apt-get install -y git
sudo apt-get install -y python-pip
sudo apt-get install -y python-zmq
sudo apt-get install -y python-mysqldb
sudo apt-get install -y python-Levenshtein
sudo apt-get install 7z
sudo pip install -r requirements.txt
```

Executing sample scripts

    sqlite3 alchemy.db < ~/patentprocessor/lib/alchemy/counts.sql

Personal Notes

  * [Adding Indices](http://stackoverflow.com/questions/6626810/multiple-columns-index-when-using-the-declarative-orm-extension-of-sqlalchemy)
  * [Ignoring Files](https://help.github.com/articles/ignoring-files)
