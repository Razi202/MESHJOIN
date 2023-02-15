# MESHJOIN
This is the implementation of the MeshJoin algorithm, which served as a pioneer for optimizing semi-stream join operations in Data Warehousing (DW).

In the provided code, multiple stream tuples and master data tuples from the disk are joined in a single iteration.
The stream tuples are mapped in a multi-valued hashmap having product ID (PID) as a key and against it are all of its tuples in a single iteration.
The master data records are also loaded and if there are matching PIDs, then the tuples from the hashmap and master data will be joined. With each iteration, the stream tuples are appended in a queue and whenever the tuples are joined, they are deleted from the queue.
The size of the queue needs to be equal to the partitions of master data (each parition contains multiple tuples).
If the size of the queue is full, then the oldest records will be removed and new will take their place from the hashmap.

The join algorithm is used to populate a DW with the star schema given in the repository. The queries to create and populate the DW via MeshJoin are also provided in the code.

The SQL file consists of some OLAP queries to answer some FAQs and test if the star schema functions.

## Steps needed to run the code
-> I created an extra DDL function in my java file that creates tables when you will run the file. Comment it if you only want to use the DDL SQL file.

-> For the java file to run, you must include the two jar files I included in the folder (if you don't already have them).

-> To include jar files, go the your project's 'properties' option and use 'Java Build Path'.

-> To connect to your database, you will have to input your schema name, username, and password. (You won't have to edit anything, the code will ask you)

-> There is "package SQL" on the top of the code. Change it according to you own project or the code might not run.
