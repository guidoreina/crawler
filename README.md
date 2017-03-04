Crawler
=======
`Crawler` is a crawler implemented in Java.

The URLs are stored in a database where three tables are maintained:

* `VISITED_URLS`: table of visited URLs. It has three columns:
  * `URL`: URL which was visited.
  * `TIMESTAMP`: timestamp when the URL was visited.
  * `FILENAME`: name of the data file containing the response.

* `VISITED_HOSTS`: table of visited hosts. It has three columns:
  * `HOST`: host which was visited.
  * `TIMESTAMP`: timestamp when the host was last visited.
  * `SERVER`: HTTP header `Server`.

* `URLS_TO_VISIT`: table of URLs to visit. It has three columns:
  * `URL`: URL to visit.
  * `HOST`: URL's host.
  * `WHEN`: the earliest time when the URL can be visited.

The `derby` database is used and can be used either as an embedded database or as a server.

The crawler's main loop is the following:
```
  do:
    url = next URL to visit
    if url is not null:
      make HTTP request and save response in a data file

      save URL in the table of visited URLs
      save the hostname in the table of visited hosts

      if the Content-Type is "text/html":
        process data file to extract links

      remove URL from the table of URLs to visit
  while running
```

The crawler takes care not to perform consecutive requests without delay to the same host. The constant `HOST_VISIT_INTERVAL` in the class `Database` defines the minimum interval in which a host will be visited.

The crawler's usage is:
```
Options:
  --host <host>
  --port <port>
  --database-name <database-name> (default: urlsDB).
  --temp-dir <directory> (default: tmpdata).
  --final-dir <directory> (default: data).
  --user-agent <user-agent> (default: Mozilla/5.0 (X11; Linux x86_64; rv:38.0) Gecko/20100101 Firefox/38.0 Iceweasel/38.7.1).
  --log-filename <log-filename> (default: crawler.log).
  --log-level <log-level> (default: FINEST).
```

If no host and port are provided, the derby embedded driver is used; otherwise the client driver.

There are no URLs to visit the first time the crawler is started. Use the `Database` class to add a URL to the table of URLs to visit. The usage is:

```
Usage: [OPTIONS] <action> [<arguments>]

Options:
  --host <host>
  --port <port>
  --database-name <database-name>

Actions:
  --view-tables
  --view-table-visited-urls
  --view-table-visited-hosts
  --view-table-urls-to-visit
  --drop-tables
  --drop-table-visited-urls
  --drop-table-visited-hosts
  --drop-table-urls-to-visit
  --add-url-to-visit <URL>
  --remove-url-to-visit <URL>
```

Example:
```
java --host localhost --port 1527 --database-name urlsDB --add-url-to-visit "http://www.example.com/"
```

To start the crawler, you can use the following command:
```
java -Djava.util.logging.SimpleFormatter.format='%1$tY/%1$tm/%1$td %1$tH:%1$tM:%1$tS [%4$s] %5$s%n' Crawler --host localhost --port 1527
```
