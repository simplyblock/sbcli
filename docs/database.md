## Create database workloads

add a new database
```
sbcli-dev -d database add --type postgresql --version 15.13 --vcpu_count 2 --memory_size 4G --disk_size 100G --storage_class  simplyblock-csi-sc postgres1
```

By default this will create a database connect to the database
```
kubectl run -it --rm psql-client --image=postgres -- bash
```

connect to DB with password password
psql -h postgres1-clone-svc -U simplyblock_admin -d simplyblock_db
```

```
CREATE TABLE Persons (
    PersonID int,
    LastName varchar(255),
    FirstName varchar(255),
    Address varchar(255),
    City varchar(255)
);

INSERT INTO Persons (PersonID, LastName, FirstName, Address, City) VALUES(1, 'Jhon', 'Doe', 'BakerStreet', 'London');
```
### create a snapshot

```
usage: sbcli-dev database snapshot [-h] database_id snapshot_name
```

sbcli-dev db snapshot 6cfd6dfe-ed13-407d-8431-83fb42ce51ac postgres1-snapshot

and the snapshots can be viewed
```
sbcli-dev db list-snapshots 6cfd6dfe-ed13-407d-8431-83fb42ce51ac
```

