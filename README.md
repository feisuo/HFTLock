# HFTLock
A highly highly fault-tolerant distributed lock service modeled after Google's Chubby [1]

## How To Build

git clone https://github.com/ralgond/libpax.git  
  
cd libpax  
  
mvn clean package install  
  
cd ..  
  
git clone https://github.com/ralgond/HFTLock.git  
  
cd HFTLock

mvn clean package

## How To Run
ht.lock.example.LockServiceCluster - How to run a lock service cluster on a single machine.  
  
ht.lock.example.LockServiceCell - How to run a lock service cell.  
  
ht.lock.example.Client03 - How to run a lock service client.  

## HFTLock Software Stack

## References
[1] Mike Burrows. [The chubby lock service for loosely-coupled distributed systems](https://www.usenix.org/legacy/event/osdi06/tech/full_papers/burrows/burrows.pdf). In OSDI ’06: Proceedings of the 7th symposium on Operating systems design and implementation, pages 335–350, Berkeley, CA, USA, 2006. USENIX Association.  
