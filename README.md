# reveal-storm-modules
A set of media processing modules that run on top of Storm and their HTTP controller.

This repo contains two projects:

* storm-modules contains the CERTH indexing and similarity modules that run on top of Storm. This includes the corresponding spouts, bolts, topologies, runners as well as examples for testing.
* http-endpoint contains a simple web service, which is responsible for submitting and killing topologies, as well as returning their current status.

