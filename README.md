# Patterdale [![Build Status](https://travis-ci.org/tjheslin1/Patterdale.svg?branch=master)](https://travis-ci.org/tjheslin1/Patterdale)

[![Docker Pulls](https://img.shields.io/docker/pulls/tjheslin1/patterdale.svg?maxAge=604800)](https://hub.docker.com/r/tjheslin1/patterdale/)

`docker run -d -p 7000:7000 -v /your/jdbc/odjbc7.jar:/app/odjbc7.jar -v /your/config/directory:/config -v /your/secrets/directory:/passwords tjheslin1/patterdale:0.16.0`

If a `logback.xml` file is included in the directory passed into the /config container volume, this will configure your logging.

[See here for documentation](https://tjheslin1.github.io/Patterdale/)
