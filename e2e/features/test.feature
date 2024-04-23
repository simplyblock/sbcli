Feature: fio tests

Scenario: Node outrage

Given a volume is created
When a node with no lvols connected is $state
Then the node is in offline status
Then all the nodes are online
Then all the nodes are online
Then the cluster is in degraded state
Then the cluster is in degraded state
Then the devices of the node are in unavailable state
Then the event log contains the records indicating the object status changes
Then check if fio is still running


state
|suspended|
|inactive|
