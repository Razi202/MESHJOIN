-- DIMENSIONS
drop table if exists products,customers,stores, suppliers, fact, dates;

create table PRODUCTS (PRODUCT_ID varchar(6), PRODUCT_NAME varchar(30), PRICE DECIMAL(5,2) DEFAULT (0.0),primary key (PRODUCT_ID));
create table CUSTOMERS (CUSTOMER_ID varchar(4), CUSTOMER_NAME varchar(30),primary key (CUSTOMER_ID));
create table STORES (STORE_ID varchar(4), STORE_NAME varchar(20),primary key (STORE_ID));
create table SUPPLIERS (SUPPLIER_ID varchar(5), SUPPLIER_NAME varchar(30),primary key (SUPPLIER_ID));
create table DATES (DATE_ date, D numeric(2), M numeric(2), QUART numeric(1), Y numeric(4), primary key (DATE_, D, M, QUART, Y));

-- FACT TABLE

create table FACT (
TRANSACTION_ID INT,
PRODUCT_ID varchar(6),
QUANTITY SMALLINT,
CUSTOMER_ID varchar(4), 
STORE_ID varchar(4), 
SUPPLIER_ID varchar(5),
Date_ date, D numeric(2), M numeric(2), QUART numeric(1),Y numeric(4), 
SALE_MONEY DECIMAL(5,2) DEFAULT (0.0),
foreign key (PRODUCT_ID) references PRODUCTS(PRODUCT_ID),
foreign key (CUSTOMER_ID) references CUSTOMERS(CUSTOMER_ID),
foreign key (STORE_ID) references STORES(STORE_ID),
foreign key (SUPPLIER_ID) references SUPPLIERS(SUPPLIER_ID),
foreign key (DATE_, D, M, QUART, Y) references DATES(DATE_, D, M, QUART, Y)
); 


-- drop table fact;
-- drop table products;
-- drop table suppliers;
-- drop table customers;
-- drop table stores;
