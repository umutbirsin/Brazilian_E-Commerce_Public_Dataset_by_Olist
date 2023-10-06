create database Brazilian_E_Commerce ;

create table orders
(
	order_id varchar ,
	customer_id varchar,
	order_status varchar,
	order_purchase_timestamp timestamp,
	order_approved_at timestamp,
	order_delivered_carrier_date timestamp,
	order_delivered_customer_date timestamp,
	order_estimated_delivery_date timestamp
)

;

create table order_items
(
	order_id varchar,
	order_item_id integer, 
	product_id varchar,
	seller_id varchar,
	shipping_limit_date timestamp,
	price float,
	freight_value float
)

;

create table order_payments
(
	order_id varchar,
	payment_sequential integer,
	payment_type varchar,
	payment_installments integer,
	payment_value float
)

;

create table order_reviews
(
	review_id varchar,
	order_id varchar,
	review_score integer,
	review_comment_tittle varchar,
	review_comment_message text,
	review_creation_date timestamp,
	review_answer_timestamp timestamp	
)

;

create table products
(
	product_id varchar,
	product_category_name varchar,
	product_name_lenght  integer,
	product_description_lenght integer,
	product_photos_qty  integer,
	product_weight_g  integer,
	product_lenght_cm  integer,
	product_height_cm  integer,
	product_width_cm   integer
)

;

create table customers
(
	customer_id varchar,
	customer_unique_id  varchar,
	customer_zipcode_prefix integer,
	customer_city varchar,
	customer_state  varchar

)

;

create table sellers
(
	seller_id  varchar,
	seller_zip_code_prefix integer,
	seller_city   varchar,
	seller_state  varchar
)

;

copy customers from '/Library/PostgreSQL/15/bin/olist_customers_dataset.csv' delimiter ',' csv header
copy order_items from '/Library/PostgreSQL/15/bin/olist_order_items_dataset.csv' delimiter ',' csv header
copy order_payments from '/Library/PostgreSQL/15/bin/olist_order_payments_dataset.csv' delimiter ',' csv header
copy order_reviews from '/Library/PostgreSQL/15/bin/olist_order_reviews_dataset.csv' delimiter ',' csv header
copy orders from '/Library/PostgreSQL/15/bin/olist_orders_dataset.csv' delimiter ',' csv header
copy products from '/Library/PostgreSQL/15/bin/olist_products_dataset.csv' delimiter ',' csv header
copy sellers from '/Library/PostgreSQL/15/bin/olist_sellers_dataset.csv' delimiter ',' csv header

select seller_id,count(seller_id) from sellers group by 1 order by 2 asc
ALTER TABLE customers ADD CONSTRAINT customers_pkey PRIMARY KEY (customer_id);
ALTER TABLE orders ADD CONSTRAINT orders_pkey PRIMARY KEY (order_id);
ALTER TABLE products ADD CONSTRAINT products_pkey PRIMARY KEY (product_id);
ALTER TABLE sellers ADD CONSTRAINT sellers_pkey PRIMARY KEY (seller_id);
ALTER TABLE orders ADD CONSTRAINT orderss__fkey FOREIGN KEY (customer_id) REFERENCES customers (customer_id);
ALTER TABLE order_reviews ADD CONSTRAINT order_reviews_fkey FOREIGN KEY (order_id) REFERENCES orders (order_id;)
ALTER TABLE order_items ADD CONSTRAINT orders_items_fkey FOREIGN KEY (order_id) REFERENCES orders (order_id);
ALTER TABLE order_payments ADD CONSTRAINT orders_payments_fkey FOREIGN KEY (order_id) REFERENCES orders (order_id);
ALTER TABLE order_items ADD CONSTRAINT orders_items_fkey_2 FOREIGN KEY (product_id) REFERENCES products (product_id);
ALTER TABLE order_items ADD CONSTRAINT orders_items_fkey_3 FOREIGN KEY (seller_id) REFERENCES sellers (seller_id);
;


--Case 1: Order Analysis
--Question 1:
--Examine the monthly order distribution. Use order_approved_at for date information.
select to_char(order_approved_at,'YYYY-MM') as order_month, 
		count(order_id) as order_count
		from orders 
		where order_approved_at is not null
group by 1
order by 1
;

--Question 2:
--Examine the order counts by order status on a monthly basis.
select to_char(order_approved_at,'YYYY-MM') as order_month, 
		order_status as order_status,
		count(order_id)
		from orders
		where to_char(order_approved_at,'YYYY-MM') is not null and order_status='shipped'
		group by 1,2
		order by 1
--Question 3:
--Examine order counts by product category. Which categories stand out on special occasions like New Year's Eve or Valentine's Day?
select to_char(o.order_approved_at,'YYYY-MM'),p.product_category_name, count(distinct o.order_id) from orders as o 
left join order_items as oi on o.order_id=oi.order_id
left join products as p on oi.product_id=p.product_id
where to_char(o.order_approved_at,'MM-DD') between '02-01' and '02-14'
group by 1,2
order by 3 desc

--Question 4:
--Analyze order counts based on the days of the week (e.g., Monday, Thursday) and the days of the month (e.g., 1st, 2nd).
select extract(day from(order_approved_at)) as day_of_month, count(distinct order_id) as order_count
	from orders
group by 1 
order by 1
;
select  to_char(order_approved_at,'Day') as day_of_week, count(distinct order_id) as order_count
	from orders
group by 1
order by 2 desc
;		
		
--Case 2: Customer Analysis
--Question 1:
--In which cities do customers shop the most? Determine the city where the customer places the most orders.
--(BASED ON WHERE THEY SPENT THE MOST MONEY)--
WITH customers_city_and_orders as 
      (
        select cs.customer_unique_id,
               cs.customer_city,
               count(o.order_id) as order_count
          from customers cs
          left join orders o
            on o.customer_id = cs.customer_id
         group by 1,
                  2
         order by 3 desc
       ),
       customer_city as 
       (
        select customer_unique_id,
               customer_city,
               order_count,
               row_number() over(partition by customer_unique_id order by order_count desc) as rn
          from customers_city_and_orders
       ),
       customer_city_final as 
       (
       select customer_unique_id, customer_city from customer_city where rn=1
       )
select cs.customer_city,
       count(distinct o.order_id) as order_count
  from customer_city_final as cs
  left join customers as c
    on c.customer_unique_id = cs.customer_unique_id
  left join orders as o
    on c.customer_id = o.customer_id
 group by 1
 order by 2 desc
;

--Question 2:
--Analyze order categories on a customer basis. Calculate the order category percentage for each customer.
--For example, if customer X has 20 orders and this customer placed 10 of them (50%) in the fashion category,
--6 of them (30%) in the cosmetics category, and 4 of them (20%) in the food category.
with order_count_per_customer as (
select c.customer_unique_id as id, 
	count(distinct order_id) as order_count
from customers as c
left join orders as o on c.customer_id=o.customer_id
group by 1 
order by 2 desc
), category_based_order_count as 
(
select c.customer_unique_id as id,
	p.product_category_name as category,
	count(distinct o.order_id) as category_order_count
from customers as c
left join orders as o on c.customer_id=o.customer_id
left join order_items as oi on o.order_id=oi.order_id
left join products as p on oi.product_id=p.product_id
group by 1,2
)

select s.id ,
k.category,
k.category_order_count,
s.order_count,
round(((k.category_order_count*1.0/s.order_count*1.0)),2)*100 as percentage
from order_count_per_customer as s left join category_based_order_count as k on s.id=k.id
order by percentage 
;

--Case 3: Seller Analysis
--Question 1:
--Who are the sellers that deliver orders to customers most quickly? List the top 5 sellers.
--Analyze these sellers' order counts and reviews and ratings for their products.
--(The top 5 sellers with the fastest average order delivery times are considered based on the data provided.
--When filtered to sellers with more than 1 order, the top 5 sellers only have 2 orders each.
--Since I didn't want to arbitrarily set the order count, I chose this method.)
--Top 5 orders and sellers with the fastest delivery
select  distinct o.order_id as order_id,
		seller_id,
		order_purchase_timestamp,
		order_delivered_customer_date,
		AGE (order_delivered_customer_date,order_purchase_timestamp) AS delivery_time
		from orders as o left join order_items as oi on o.order_id=oi.order_id
		order by 5 asc
		limit 5
--Average review score and order counts for the top 5 sellers
	select seller_id,
			count(distinct o.order_id),
			round((avg(review_score)),2)
			from order_items as o left join order_reviews as orr on o.order_id=orr.order_id
			where seller_id in ('3b15288545f8928d3e65a8f949a28291','f8db351d8c4c4c22c6835c19a46f01b0','fdb9095204a334cd8872252ffec6f2db','c847e075301870dd144a116762eaff9a','46dc3b2cc0980fb8ec44634e21d2718e')
			group by 1
--Reviews received by the top 5 sellers
			select seller_id,
			review_comment_message
			from order_items as o left join order_reviews as orr on o.order_id=orr.order_id
			where seller_id in ('3b15288545f8928d3e65a8f949a28291','f8db351d8c4c4c22c6835c19a46f01b0','fdb9095204a334cd8872252ffec6f2db','c847e075301870dd144a116762eaff9a','46dc3b2cc0980fb8ec44634e21d2718e')
			group by 1,2
			
---- Average seller review			
	select
			round((avg(review_score)),2)
			from order_items as o left join order_reviews as orr on o.order_id=orr.order_id

--Question 2:
--Which sellers offer products in more categories? Do sellers with more categories also have more order counts?
--Sorting by order count.
with seller_data as (
	select s.seller_id as seller_id, 
			count(distinct p.product_category_name) as category_count,
			count(distinct o.order_id) as order_count 
	from sellers as s left join order_items as oi on s.seller_id=oi.seller_id
	left join products as p on oi.product_id=p.product_id
	left join orders as o on oi.order_id=o.order_id
	group by s.seller_id order by 2 desc
), seller_averages as (
select	seller_id,
		round((select avg(category_count) from seller_data),2) as avg_cat,	
		round((select avg(order_count) from seller_data),2) as avg_ord
	from seller_data
)
select d.seller_id,
		o.avg_cat,
		d.category_count,
		o.avg_ord,
		d.order_count
		from seller_data as d left join seller_averages as o on d.seller_id=o.seller_id
		order by 5 desc
;

--Sorting by category count
with seller_data as (
	select s.seller_id as seller_id, 
			count(distinct p.product_category_name) as category_count,
			count(distinct o.order_id) as order_count 
	from sellers as s left join order_items as oi on s.seller_id=oi.seller_id
	left join products as p on oi.product_id=p.product_id
	left join orders as o on oi.order_id=o.order_id
	group by s.seller_id order by 2 desc
), seller_averages as (
select	seller_id,
		round((select avg(category_count) from seller_data),2) as avg_cat,	
		round((select avg(order_count) from seller_data),2) as avg_ord
	from seller_data
)
select d.seller_id,
		o.avg_cat,
		d.category_count,
		o.avg_ord,
		d.order_count
		from seller_data as d left join seller_averages as o on d.seller_id=o.seller_id
		order by 3 desc

--Case 4: Payment Analysis
--Question 1:
--In which regions do users with more installment payments live? Interpret the output.
--(Solving it by count)
with all_data as (
select  o.order_id,
		payment_installments as pi,
		customer_city as city
from customers as c
left join orders as o on c.customer_id=o.customer_id
left join order_payments as op on o.order_id=op.order_id
where payment_type='credit_card' and payment_installments between 4 and 10
order by 2 desc
), all_data_2 as (
select pi,
city,
count(city),
row_number() over(partition by pi order by (count(city))desc) as rn
from all_data group by 1,2 order by 1,4
)
select * from all_data_2 where rn in (1,2,3)
;;

--Average installment is 3.5, so I considered 4 and above as installment payments.
--Only taking into account orders with more than 1 installment, we wouldn't get a different output by city from the list of regions with the most orders,
--so I chose to calculate the ratio of orders with 4 or more installments to the total number of orders in each region.

--Percentage solution
with percentage_data as (
select 	customer_city as customer_city,
		count(case when op.payment_installments>=4 then o.order_id end) as more_than_4,
		count(o.order_id) total_order_count
from customers as c
left join orders as o on c.customer_id=o.customer_id
left join order_payments as op on o.order_id=op.order_id
group by 1 order by 2 desc
)
select customer_city,
		more_than_4,
		total_order_count,
		round(((more_than_4)*1.0/(total_order_count)*1.0),2)
		from percentage_data
		where total_order_count >=100
		order by 4 desc
		;

--Question 2:
--Calculate the number of successful orders and the total successful payment amount by payment type.
--Sort from the most used payment type to the least used.
--(Orders with non-null approved_at are considered successful payments.)
with all_data as (
select payment_type,
count(o.order_id) as successful_orders,
sum(payment_value) as total_amount
from orders as o 
inner join order_payments as op on o.order_id=op.order_id
where o.order_approved_at is not null
group by payment_type  order by 2 desc
)
select payment_type,
		successful_orders,
		round((select sum(total_amount) from all_data)) as total_amount
		from all_data
;

--Question 3:
--Analyze orders paid in a single installment and with installments by category.
select product_category_name,count(distinct op.order_id) as order_count,
	round (avg(payment_value)) as avg_amount,
	round (sum(payment_value)) as total_amount
from order_items as oi left join products as p on oi.product_id=p.product_id
left join order_payments as op on op.order_id=oi.order_id
where payment_installments<=1
group by 1
	order by order_count desc

--In which categories are installment payments most commonly used?
select product_category_name,
	count(distinct op.order_id) as order_count,
	round (avg(payment_value)) as avg_amount,
	round (sum(payment_value)) as total_amount
from order_items as oi left join products as p on oi.product_id=p.product_id
left join order_payments as op on op.order_id=oi.order_id
where payment_installments>1
group by 1
	order by order_count desc