# BankingApplication-Chandy-Lamport-Snapshot
Using Google’s protocol buffer for marshalling and unmarshalling messages and TCP sockets for sending and receiving messages.

Description:

BranchServer.java
It included the logic specific to each Branch

Controller.java
The controller initiates snapshot

Bank.java
This file is created by .proto file (google protocol buffer)

Steps:

$make

It will compile all the files and initializes the branch on given port number
$.branch.sh <branch-name> <port-number>

This way we can start a branch on a specified port
$controller.sh <amount> <filename>

Amount is distributed evenly among all the branches. Information of all branches is read from <filename>

