README
======

There are a few items that are required:

    sudo apt-get install -y git
    sudo apt-get install -y python-pip
    sudo apt-get install -y python-zmq
    sudo apt-get install -y python-mysqldb
    sudo pip install -r requirements.txt

Executing Some sample scripts

    sqlite3 alchemy.db < ~/patentprocessor/lib/alchemy/counts.sql

Functions

  * [Adding Indices](http://stackoverflow.com/questions/6626810/multiple-columns-index-when-using-the-declarative-orm-extension-of-sqlalchemy)
