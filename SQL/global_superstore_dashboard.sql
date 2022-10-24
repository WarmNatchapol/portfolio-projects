----- Pattern of Code -----
Create Table (CREATE TABLE)
Insert Data (INSERT INTO)
Check duplicate rows and delete if there has duplicated. (WITH cte AS)


----- Create Tables -----
-- products --
CREATE TABLE products(
	product_id int,
	product_name nvarchar(255),
	category nvarchar(255),
	sub_category nvarchar(255)
);

INSERT INTO products
SELECT 
	new_product_id, 
	product_name, 
	category, sub_category
FROM new_global_store;

WITH cte AS(
	SELECT
		*,
		ROW_NUMBER() OVER(
			PARTITION BY 
				product_id, 
				product_name, 
				category, 
				sub_category
			ORDER BY 
				product_id, 
				product_name, 
				category, 
				sub_category
		) AS row_num
	FROM products
)

DELETE FROM cte 
WHERE row_num > 1;


-- customers --
CREATE TABLE customers(
	customer_id nvarchar(255),
	customer_name nvarchar(255),
	segment nvarchar(12)
);

INSERT INTO customers
SELECT 
	customer_id, 
	customer_name, 
	segment 
FROM new_global_store;

WITH cte AS(
	SELECT
		*,
		ROW_NUMBER() OVER(
			PARTITION BY 
				customer_id, 
				customer_name, 
				segment 
			ORDER BY 
				customer_id, 
				customer_name, 
				segment 
		) AS row_num
	FROM customers
)
DELETE FROM cte 
WHERE row_num > 1;


-- customers_address --
CREATE TABLE customers_address(
	row_id int IDENTITY(1,1) PRIMARY KEY,
	customer_id nvarchar(255),
	country nvarchar(255),
	state nvarchar(255),
	city nvarchar(255)
);

INSERT INTO customers_address (customer_id, country, state, city)
SELECT 
	customer_id, 
	country, 
	state, 
	city 
FROM new_global_store;

WITH cte AS(
	SELECT
		*,
		ROW_NUMBER() OVER(
			PARTITION BY 
				customer_id, 
				country, 
				state, 
				city 
			ORDER BY 
				customer_id, 
				country, 
				state, 
				city
		) AS row_num
	FROM customers_address
)
DELETE FROM cte 
WHERE row_num > 1;


-- transactions --
CREATE TABLE transactions(
	row_id int,
	order_id nvarchar(255),
	order_date date,
	ship_date date,
	ship_mode nvarchar(255),
	customer_id nvarchar(255),
	market nvarchar(10),
	region nvarchar(20),
	product_id int,
	sales real,
	quantity int,
	discount real,
	profit real,
	shipping_cost real,
	order_priority nvarchar(8)
);

INSERT INTO transactions 
SELECT 
	row_id, 
	order_id, 
	CONVERT(date, order_date, 105), 
	CONVERT(date, ship_date, 105), 
	ship_mode, 
	customer_id, 
	market, 
	region, 
	new_product_id, 
	sales, 
	quantity, 
	discount, 
	profit, 
	shipping_cost, 
	order_priority
FROM new_global_store;

WITH cte AS(
	SELECT
		*,
		ROW_NUMBER() OVER(
			PARTITION BY 
				row_id, 
				order_id, 
				order_date, 
				ship_date, 
				ship_mode, 
				customer_id, 
				market, 
				region, 
				product_id, 
				sales, 
				quantity, 
				discount, 
				profit, 
				shipping_cost, 
				order_priority
			ORDER BY 
				row_id, 
				order_id, 
				order_date, 
				ship_date, 
				ship_mode, 
				customer_id, 
				market, 
				region, 
				product_id, 
				sales, 
				quantity, 
				discount, 
				profit, 
				shipping_cost, 
				order_priority
		) AS row_num
	FROM transactions
)
DELETE FROM cte 
WHERE row_num > 1;
