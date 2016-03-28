Unsatisfied with current situation is this a Delphi 5 port of MongoDB's c-driver(version 1.3.4). It requires the 32bit compiled versions of libmongo.dll and mongoc.dll. Nearly untested and implements only a handful features/functions but a good start.

Currently missing parts:
* many function which i don't needed
* no bcon support
* no gridfs support

ToDo:
* documentation
* full test case coverage
* resolve more pointer to typed pointer
* seperate class mongodb


License:
Like the c-driver Apache 2.0.
