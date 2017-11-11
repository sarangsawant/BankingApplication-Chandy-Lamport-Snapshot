The source files are located in /src folder.
A bash script server.sh is included to start the Branch server and controller.sh to start controller

Steps to execute code:
1. Execute make command. It creates /bin folder. All the .class executables are place in bin folder
2. Start branch server using command "./branch.sh <branch_name> <port_number>". 
3. Start controller using command "./controller.sh <amount> <path of text file>"

----------------------------------
1. Execuring ./controller.sh, controller sends init branch message to all branches.
2. After every 2sec interval, controller sends InitSnapshot message to one of the randomly selected branch(SnapshotId starting with 1).
3. For each initSnapshot message send by controller, this message is followed by Retrieve snapshot message to all branches. Controller waits for Return snapshot message
   from all branches in different thread
