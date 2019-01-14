-- add all your SQL setup statements here. 

-- You can assume that the following base table has been created with data loaded for you when we test your submission 
-- (you still need to create and populate it in your instance however),
-- although you are free to insert extra ALTER COLUMN ... statements to change the column 
-- names / types if you like.

--FLIGHTS (fid int, 
--         month_id int,        -- 1-12
--         day_of_month int,    -- 1-31 
--         day_of_week_id int,  -- 1-7, 1 = Monday, 2 = Tuesday, etc
--         carrier_id varchar(7), 
--         flight_num int,
--         origin_city varchar(34), 
--         origin_state varchar(47), 
--         dest_city varchar(34), 
--         departure_delay int, -- in mins
--         taxi_out int,        -- in mins
--         arrival_delay int,   -- in mins
--         canceled int,        -- 1 means canceled
--         actual_time int,     -- in mins
--         distance int,        -- in miles
--         capacity int, 
--         price int            -- in $             
--         )

CREATE TABLE Users (
	username VARCHAR(20) NOT NULL PRIMARY KEY,
	pass VARCHAR(20),
	balance INT
);

CREATE TABLE Reservations (
	rid INT NOT NULL PRIMARY KEY,
	fid1 INT NOT NULL REFERENCES Flights(fid),
	fid2 INT, -- -1 for no flight 2
	usr VARCHAR(20) REFERENCES Users(username),
	paid INT, -- 0 for un-pay, 1 for paid
	cost INT,
	day INT	
);

CREATE TABLE Capacities (
	fid INT PRdaIMARY KEY REFERENCES Flights(fid),
	capacity INT
);

CREATE TABLE ReserveCount (
	count int NOT NULL PRIMARY KEY 
);
