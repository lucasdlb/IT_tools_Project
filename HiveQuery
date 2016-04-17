use default;

-- Creation of the tables --

DROP TABLE IF EXISTS catalogue;
CREATE EXTERNAL TABLE catalogue (
					  ProductColorId STRING, 
                      Gender_Label STRING,
                      SupplierColorLabel STRING, 
                      SeasonLabel STRING)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\073' 
STORED AS TEXTFILE
LOCATION "/tmp/projet_it/catalogue/"
TBLPROPERTIES ("skip.header.line.count"="1");

DROP TABLE IF EXISTS customer;
CREATE EXTERNAL TABLE customer (
					   CustomerId STRING,
                       DomainCode STRING,
                       BirthDate DATE,
                       Gender STRING,
                       Size int)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\073' 
STORED AS TEXTFILE
LOCATION "/tmp/projet_it/customer/"
TBLPROPERTIES ("skip.header.line.count"="1");

DROP TABLE IF EXISTS all_order;
CREATE EXTERNAL TABLE all_order (
					OrderNumber STRING,
                    Variant_Id STRING, 
                    Customer_Id STRING,
                    Quantity INT,
                    Unit_Price DOUBLE,
                    order_date STRING)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\073' 
STORED AS TEXTFILE
LOCATION "/tmp/projet_it/all_order/"
TBLPROPERTIES ("skip.header.line.count"="1");

DROP TABLE IF EXISTS references;
CREATE EXTERNAL TABLE references (
					VariantId STRING,
                    ProductColorId STRING,
                    ProductId STRING)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\073' 
STORED AS TEXTFILE
LOCATION "/tmp/projet_it/references//"
TBLPROPERTIES ("skip.header.line.count"="1");

DROP TABLE IF EXISTS variant;
CREATE EXTERNAL TABLE variant (
					  VariantId STRING,
                      MinSize int,	
					  MaxSize int,
                      Size STRING)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\073' 
STORED AS TEXTFILE
LOCATION "/tmp/projet_it/variant/"
TBLPROPERTIES ("skip.header.line.count"="1");





-- table 1 PRODUCT AGGREGATION

DROP TABLE IF EXISTS product_aggragation;
CREATE TABLE IF NOT EXISTS product_aggragation
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '/073' 
STORED AS TEXTFILE
LOCATION "/tmp/projet_it/result_product_aggragation/"
 AS
select 
	prod.productid as product_id, 
	sum(prod.quantity) as total_quantity_sold, 
	sum(prod.quantity * prod.unit_price) as total_amount_sold,
	count(distinct customer_id) as number_distinct_custumers  
		from(
			select 
		  		all_order.customer_id, 
		  		all_order.quantity, 
		  		all_order.unit_price, 
		    	references.productid 
		  	from all_order
			left outer join references on all_order.variant_id = references.variantid) prod
group by prod.productid;

--





-- table 2 CUSTOMER AGGREGATION

--   Three steps 1: creation of the domnant size table 
--               2: creation of the domnant size table 
--               3: join both above with customerid and the last purchase date by cutumer id


-- 1

DROP TABLE IF EXISTS dominant_size;
CREATE TABLE dominant_size 
AS 
select -- select customerid and the maximum size of the partinonned table with already just dominant size or tie size ordered
	customer_id,
	max(size) as most_purchase_size
from(
	select * from( -- select just the customer with rank = 1 i.e dominant size or tie
  		select -- create partinonned table over the customer ordered by number of purchase of a particular size
  			*,
  			rank() over (partition by customer_id order by size_count desc, last_date_order_for_this_size desc) as rk
  		from( 
			select -- aggregate the table by customer id and size 
				size_table.customer_id,
				size_table.size,
				count(size_table.size) as size_count,
				max(size_table.order_date) as last_date_order_for_this_size
			from(			
				select -- join customer table and order table
	  				all_order.customer_id, 
		  			all_order.order_date,
	  				variant.size		  
				from all_order
				left outer join variant on all_order.variant_id = variant.variantid) size_table
			group by size_table.customer_id, size_table.size) size_table_count
		) size_table_count_partionned
	where size_table_count_partionned.rk = 1) 
ranked_size_table
group by customer_id;

-- 1 end

-- 2 

DROP TABLE IF EXISTS dominant_gender;
CREATE TABLE dominant_gender
AS 
select -- select customerid and the gender of the partinonned table with already just dominant gender or tie
	customer_id,
	gender_label as most_purchase_gender 
from(
	select -- create partinonned table over the customerid ordered by ordinal transformation of the gender
		*,
  		row_number() over (partition by customer_id order by ordinal_gender asc) as rn 
	from(
		select -- create a column giving the gender as an ordinal value to deals with ties
			*,
			case 
				when gender_label = 'Femme' then 1
				when gender_label = 'Homme' then 2 
				when gender_label = 'Enfant' then 3
				when gender_label = 'Sacs' then 4
				when gender_label = 'Accesoires' then 5
				else 6 end ordinal_gender
		from(
			select * from( -- select just the customer with rank = 1 i.e dominant gender or tie
			  	select -- create partinonned table over the customerid ordered by number of purchase of a particular gender
					*,
  					rank() over (partition by customer_id order by gender_count desc, last_date_order_for_this_size desc) as rk 
				from(
					select -- aggregation by customerid and gender 
						gender_table.customer_id,
						gender_table.gender_label,
						count(gender_table.gender_label) as gender_count,
						max(gender_table.order_date) as last_date_order_for_this_size
					from(
						select -- join the order and the gender
							all_order.customer_id, 
							all_order.order_date,
							variant_gender.gender_label
					  	from(
						  	select -- join gender and variantid on productcolorid
								catalogue.gender_label,
								references.variantid
							from references
							left outer join catalogue on catalogue.productcolorid = references.productcolorid) variant_gender
			  
					  	join all_order on all_order.variant_id = variant_gender.variantid) gender_table
				  
					group by gender_table.customer_id, gender_table.gender_label) gender_table_count 
			  
				) gender_table_count_ranked
			
		  	where rk = 1) query_with_ties 
	
		) query_with_ties_and_ordinal_gender

) query_with_ties_and_ordinal_gender_partionned
where rn = 1;

-- 2 end

-- 3 

DROP TABLE IF EXISTS customer_aggragation;
CREATE TABLE IF NOT EXISTS customer_aggragation
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '/073' 
STORED AS TEXTFILE
LOCATION "/tmp/projet_it/result_customer_aggragation/"
AS
select -- simple join of both table created above and customer last purchase date
	last_date_table.customer_id,
	last_date_table.last_order_date,
	dominant_gender.most_purchase_gender,
	dominant_size.most_purchase_size
from (
  select 
	all_order.customer_id,
	max(all_order.order_date) as last_order_date
  from all_order
  group by all_order.customer_id) last_date_table
left outer join dominant_gender on last_date_table.customer_id = dominant_gender.customer_id
left outer join dominant_size on last_date_table.customer_id = dominant_size.customer_id;
