__author__ = 'mars'

#Using breadth-first search algorithm (BFS)

import config
import re
import mysql.connector

from mysql.connector import errorcode
from urllib.request import urlopen
from bs4 import BeautifulSoup
from collections import deque

urlopen(config.site)
name = re.compile(r'[/:.#]').sub('_', config.site)

try:
    cnx = mysql.connector.connect(**config.db)
    c = cnx.cursor()

    try:
        c.execute("""DROP TABLE IF EXISTS %s""" % name)

    except mysql.connector.Error as err:
        if err.errno != errorcode.ER_BAD_TABLE_ERROR:
            raise

    c.execute("""CREATE TABLE %s (
           id         int         NOT NULL AUTO_INCREMENT PRIMARY KEY,
           host       int         NOT NULL,
           link       char(255)   NOT NULL)""" % name)

    queue = deque([config.site])
    host = number_of_links = 0

    while True:

        page = urlopen(queue[0])
        parsed_html = BeautifulSoup(page)

        for link in parsed_html.find_all('a'):

            result = link.get('href')
            c.execute("""SELECT id FROM %s WHERE link = %%s""" % name, (result,))

            if not c.fetchone():

                try:
                    urlopen(result)

                except Exception:
                    c.execute("""INSERT IGNORE INTO %s (host, link) VALUES (%%s, %%s)""" % name,
                              (host, 'bad link: ' + result))
                else:
                    c.execute("""INSERT IGNORE INTO %s (host, link) VALUES (%%s, %%s)""" % name,
                              (host, result))
                    queue.append(result)

                number_of_links += 1
                cnx.commit()

        if number_of_links > config.number_of_links or len(queue) == 0:
            print("Work's done. Number of links = %s " % number_of_links)
            break

        queue.popleft()
        c.execute("""SELECT id FROM %s WHERE link = %%s""" % name, (queue[0],))
        host = c.fetchone()[0]

except mysql.connector.Error as err:

    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:

        print("Something is wrong with your user name or password")

    elif err.errno == errorcode.ER_BAD_DB_ERROR:

        print("Database does not exists")

    else:
        print(err)
else:
    cnx.close()
