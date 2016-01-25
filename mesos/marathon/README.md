Running in Marathon
==================

This directory contains a `template.json` file to allow running the scheduler in Marathon.

You may feed it to Marathon API via a standard `POST /v2/apps` call:

```
# curl -X POST -T template.json http://master:8080/v2/apps
```

You will definitely want to change URI list and point it to some working artifact server so that Marathon can find necessary files for the scheduler.

You might also want to change cpus/mem params but given configuration should work fine.

Constraints and ports are optional and are used to stick the scheduler to a specific host/port.