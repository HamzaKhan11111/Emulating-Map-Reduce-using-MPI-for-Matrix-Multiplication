# Emulating Map Reduce using MPI
 This project aims to utilize Message Passing Interface(MPI) in C to emulate the Map Reduce framework. Map Reduce works by having a Nodemanager which accepts a job and then splits the input to the mappers, and mappers inform the Nodemanager after the completion of their jobs. Nodemanager will shuffle the output of the mappers and send them to the reducers. 
