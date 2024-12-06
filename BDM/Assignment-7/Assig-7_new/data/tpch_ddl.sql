-- TPCH DDL

create table customer
				 (c_custkey     int                    not null primary key,
				  c_name        varchar(25)            not null,
				  c_address     varchar(40)            not null,
				  c_nationkey   int                    not null,
				  c_phone       char(15)               not null,
				  c_acctbal     decimal(15,2)          not null,
				  c_mktsegment  char(10)               not null,
				  c_comment     char(117)              not null)

create table orders
				 (o_orderkey      int                  not null primary key,
				  o_custkey       int                  not null,
				  o_orderstatus   char(1)              not null,
				  o_totalprice    decimal(15,2)        not null,
				  o_orderdate     datetime             not null,
				  o_orderpriority char(15)             not null,
				  o_clerk         char(15)             not null,
				  o_shippriority  int                  not null,
				  o_comment       varchar(79)          not null)
