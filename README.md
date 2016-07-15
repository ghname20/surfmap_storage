# surfmap_storage
## A storage system for the surfmapper project.

**Surfmapper** is a project designed to keep track of browsing history and visualize it in a node graph.  
The project's main incentive was to handle huge navigation graphs outside of the browser, because otherwise it would hog too much memory.  

This is a storage system, which currently uses JSON.  

It is capable of:  
1. collect surfing data which is sent by the chrome extension through the SOAP interface.  
2. collect surfing data from logs, written by Fiddler.  
3. execute queries on the collected data, returning subgraph data through the SOAP interface or writing it into report files.  
4. has command-line tools to query, split, merge graph storage, attach, detach subgraphs, work with browsing sessions.

Currently I have no time to describe it in detail or work out some installation procedure.
