#1
select f.SUPPLIER_ID, SUPPLIER_NAME,
sum(
case
	when QUART = 1 then QUANTITY
	else 0
	end
) as Quarter1,
sum(
case
	when QUART = 2 then QUANTITY
	else 0
	end
) as Quarter2,
sum(
case
	when QUART = 3 then QUANTITY
	else 0
	end
) as Quarter3,
sum(
case
	when QUART = 4 then QUANTITY
	else 0
	end
) as Quarter4,
sum(
case
	when M = 1 then QUANTITY
	else 0
	end
) as Month1,
sum(
case
	when M = 2 then QUANTITY
	else 0
	end
) as Month2,
sum(
case
	when M = 3 then QUANTITY
	else 0
	end
) as Month3,
sum(
case
	when M = 4 then QUANTITY
	else 0
	end
) as Month4,
sum(
case
	when M = 5 then QUANTITY
	else 0
	end
) as Month5,
sum(
case
	when M = 6 then QUANTITY
	else 0
	end
) as Month6,
sum(
case
	when M = 7 then QUANTITY
	else 0
	end
) as Month7,
sum(
case
	when M = 8 then QUANTITY
	else 0
	end
) as Month8,
sum(
case
	when M = 9 then QUANTITY
	else 0
	end
) as Month9,
sum(
case
	when M = 10 then QUANTITY
	else 0
	end
) as Month10,
sum(
case
	when M = 11 then QUANTITY
	else 0
	end
) as Month11,
sum(
case
	when M = 12 then QUANTITY
	else 0
	end
) as Month12
from fact f, suppliers sup
where f.SUPPLIER_ID = sup.SUPPLIER_ID
group by SUPPLIER_ID;

#2
select sum(QUANTITY) as TOTAL_SOLD, f.PRODUCT_ID, p.PRODUCT_NAME, f.STORE_ID, st.STORE_NAME
from fact f, products p, stores st
where f.PRODUCT_ID = p.PRODUCT_ID and f.STORE_ID = st.STORE_ID
group by STORE_ID, PRODUCT_ID 
order by STORE_NAME;

#3
select f.PRODUCT_ID, p.PRODUCT_NAME,sum(QUANTITY) as total_sold
from fact f, products p
where f.PRODUCT_ID = p.PRODUCT_ID and (weekday(f.DATE_)=6 or weekday(f.DATE_)=5)
group by PRODUCT_ID
order by sum(QUANTITY) desc 
limit 5;

#4
select f.PRODUCT_ID, p.PRODUCT_NAME,
sum(
case
	when f.QUART = 1 then f.QUANTITY
	else 0
	end
) as Quarter1,
sum(
case
	when f.QUART = 2 then f.QUANTITY
	else 0
	end
) as Quarter2,
sum(
case
	when f.QUART = 3 then f.QUANTITY
	else 0
	end
) as Quarter3,
sum(
case
	when f.QUART = 4 then f.QUANTITY
	else 0
	end
) as Quarter4
from fact f, products p
where f.PRODUCT_ID = p.PRODUCT_ID and f.Y = 2016
group by f.PRODUCT_ID;

#5
select f.PRODUCT_ID, p.PRODUCT_NAME,
sum(
case
	when f.M <= 6 then f.QUANTITY
	else 0
	end
) as YEARLY_HALF1,
sum(
case
	when f.M > 6 then f.QUANTITY
	else 0
	end
) as YEARLY_HALF2
from fact f, products p
where f.PRODUCT_ID = p.PRODUCT_ID and f.Y = 2016
group by f.PRODUCT_ID
order by f.PRODUCT_ID;

#6
select * from masterdata where PRODUCT_NAME = (
	SELECT PRODUCT_NAME FROM masterdata
	group by PRODUCT_NAME having count(PRODUCT_NAME) > 1
);
-- There is only one product name that repeats and their respective prices are drastically different


#7

create view STOREANALYSIS_MV as (
select f.STORE_ID, f.PRODUCT_ID, Sum(QUANTITY)
from fact f, products p, stores st 
where f.PRODUCT_ID = p.PRODUCT_ID and f.STORE_ID = st.STORE_ID
group by STORE_ID, PRODUCT_ID
);

select * from STOREANALYSIS_MV;


