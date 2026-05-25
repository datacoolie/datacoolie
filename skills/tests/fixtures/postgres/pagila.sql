-- Pagila — PostgreSQL port of MySQL Sakila (DVD rental sample database)
-- Schema + data only, no PL/pgSQL triggers/functions.
-- Source: https://github.com/devrimgunduz/pagila (simplified for discovery testing)

-- ============================================================================
-- SCHEMAS
-- ============================================================================
CREATE SCHEMA IF NOT EXISTS inventory;
CREATE SCHEMA IF NOT EXISTS sales;
CREATE SCHEMA IF NOT EXISTS staff;

-- ============================================================================
-- inventory schema — catalog of films, actors, categories
-- ============================================================================

CREATE TABLE inventory.language (
    language_id   SMALLINT     GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name          CHAR(20)     NOT NULL,
    last_update   TIMESTAMP    NOT NULL DEFAULT NOW()
);

CREATE TABLE inventory.category (
    category_id   SMALLINT     GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name          VARCHAR(25)  NOT NULL,
    last_update   TIMESTAMP    NOT NULL DEFAULT NOW()
);

CREATE TABLE inventory.actor (
    actor_id    SMALLINT     GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    first_name  VARCHAR(45)  NOT NULL,
    last_name   VARCHAR(45)  NOT NULL,
    last_update TIMESTAMP    NOT NULL DEFAULT NOW()
);

CREATE TABLE inventory.film (
    film_id          SMALLINT       GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    title            VARCHAR(255)   NOT NULL,
    description      TEXT,
    release_year     SMALLINT,
    language_id      SMALLINT       NOT NULL REFERENCES inventory.language(language_id),
    rental_duration  SMALLINT       NOT NULL DEFAULT 3,
    rental_rate      NUMERIC(4,2)   NOT NULL DEFAULT 4.99,
    length           SMALLINT,
    replacement_cost NUMERIC(5,2)   NOT NULL DEFAULT 19.99,
    rating           VARCHAR(10)    DEFAULT 'G',
    last_update      TIMESTAMP      NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_film_title   ON inventory.film(title);
CREATE INDEX idx_film_lang    ON inventory.film(language_id);

CREATE TABLE inventory.film_actor (
    actor_id    SMALLINT  NOT NULL REFERENCES inventory.actor(actor_id),
    film_id     SMALLINT  NOT NULL REFERENCES inventory.film(film_id),
    last_update TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (actor_id, film_id)
);

CREATE TABLE inventory.film_category (
    film_id     SMALLINT  NOT NULL REFERENCES inventory.film(film_id),
    category_id SMALLINT  NOT NULL REFERENCES inventory.category(category_id),
    last_update TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (film_id, category_id)
);

CREATE TABLE inventory.store_inventory (
    inventory_id  INT          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    film_id       SMALLINT     NOT NULL REFERENCES inventory.film(film_id),
    store_id      SMALLINT     NOT NULL,
    last_update   TIMESTAMP    NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_inventory_film  ON inventory.store_inventory(film_id);
CREATE INDEX idx_inventory_store ON inventory.store_inventory(store_id);

-- ============================================================================
-- sales schema — customers, rentals, payments
-- ============================================================================

CREATE TABLE sales.address (
    address_id  SMALLINT    GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    address     VARCHAR(50) NOT NULL,
    city        VARCHAR(50) NOT NULL,
    country     VARCHAR(50) NOT NULL,
    postal_code VARCHAR(10),
    phone       VARCHAR(20) NOT NULL DEFAULT ''
);

CREATE TABLE sales.customer (
    customer_id SMALLINT    GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    store_id    SMALLINT    NOT NULL,
    first_name  VARCHAR(45) NOT NULL,
    last_name   VARCHAR(45) NOT NULL,
    email       VARCHAR(50),
    address_id  SMALLINT    NOT NULL REFERENCES sales.address(address_id),
    active      BOOLEAN     NOT NULL DEFAULT TRUE,
    create_date TIMESTAMP   NOT NULL DEFAULT NOW(),
    last_update TIMESTAMP   NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_customer_last_name ON sales.customer(last_name);
CREATE INDEX idx_customer_store     ON sales.customer(store_id);

CREATE TABLE sales.rental (
    rental_id     INT        GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    rental_date   TIMESTAMP  NOT NULL,
    inventory_id  INT        NOT NULL REFERENCES inventory.store_inventory(inventory_id),
    customer_id   SMALLINT   NOT NULL REFERENCES sales.customer(customer_id),
    return_date   TIMESTAMP,
    staff_id      SMALLINT   NOT NULL,
    last_update   TIMESTAMP  NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_rental_date     ON sales.rental(rental_date);
CREATE INDEX idx_rental_customer ON sales.rental(customer_id);
CREATE INDEX idx_rental_inv      ON sales.rental(inventory_id);

CREATE TABLE sales.payment (
    payment_id   SMALLINT      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_id  SMALLINT      NOT NULL REFERENCES sales.customer(customer_id),
    staff_id     SMALLINT      NOT NULL,
    rental_id    INT           NOT NULL REFERENCES sales.rental(rental_id),
    amount       NUMERIC(5,2)  NOT NULL,
    payment_date TIMESTAMP     NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_payment_customer ON sales.payment(customer_id);
CREATE INDEX idx_payment_rental   ON sales.payment(rental_id);

-- ============================================================================
-- staff schema — stores and staff
-- ============================================================================

CREATE TABLE staff.store (
    store_id      SMALLINT   GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    address_id    SMALLINT   NOT NULL REFERENCES sales.address(address_id),
    last_update   TIMESTAMP  NOT NULL DEFAULT NOW()
);

-- ============================================================================
-- VIEWS
-- ============================================================================

CREATE VIEW sales.customer_list AS
SELECT
    c.customer_id,
    c.first_name || ' ' || c.last_name AS name,
    a.address,
    a.postal_code AS "zip code",
    a.phone,
    a.city,
    a.country,
    CASE WHEN c.active THEN 'active' ELSE '' END AS notes,
    c.store_id AS sid
FROM sales.customer c
JOIN sales.address a ON c.address_id = a.address_id;

CREATE VIEW inventory.film_list AS
SELECT
    f.film_id,
    f.title,
    f.description,
    c.name       AS category,
    f.rental_rate AS price,
    f.length,
    f.rating,
    string_agg(a.first_name || ' ' || a.last_name, ', ') AS actors
FROM inventory.film f
JOIN inventory.film_category fc ON f.film_id = fc.film_id
JOIN inventory.category c       ON fc.category_id = c.category_id
JOIN inventory.film_actor fa    ON f.film_id = fa.film_id
JOIN inventory.actor a          ON fa.actor_id = a.actor_id
GROUP BY f.film_id, f.title, f.description, c.name, f.rental_rate, f.length, f.rating;

CREATE VIEW sales.revenue_by_store AS
SELECT
    s.store_id,
    SUM(p.amount)     AS total_revenue,
    COUNT(p.payment_id) AS payment_count
FROM staff.store s
JOIN sales.customer c ON c.store_id = s.store_id
JOIN sales.payment  p ON p.customer_id = c.customer_id
GROUP BY s.store_id;

-- ============================================================================
-- SEED DATA
-- ============================================================================

INSERT INTO inventory.language (name) VALUES
    ('English'),('Italian'),('Japanese'),('Mandarin'),('French'),('German');

INSERT INTO inventory.category (name) VALUES
    ('Action'),('Animation'),('Children'),('Classics'),('Comedy'),
    ('Documentary'),('Drama'),('Family'),('Foreign'),('Games'),
    ('Horror'),('Music'),('New'),('Sci-Fi'),('Sports'),('Travel');

INSERT INTO inventory.actor (first_name, last_name) VALUES
    ('PENELOPE','GUINESS'),('NICK','WAHLBERG'),('ED','CHASE'),
    ('JENNIFER','DAVIS'),('JOHNNY','LOLLOBRIGIDA'),('BETTE','NICHOLSON'),
    ('GRACE','MOSTEL'),('MATTHEW','JOHANSSON'),('JOE','SWANK'),
    ('CHRISTIAN','GABLE'),('ZERO','CAGE'),('KARL','BERRY'),
    ('UMA','WOOD'),('VIVIEN','BERGEN'),('CUBA','OLIVIER'),
    ('FRED','COSTNER'),('HELEN','VOIGHT'),('DAN','TORN'),
    ('BOB','FAWCETT'),('LUCILLE','TRACY');

INSERT INTO inventory.film (title, description, release_year, language_id, rental_duration, rental_rate, length, replacement_cost, rating) VALUES
    ('ACADEMY DINOSAUR','Epic Drama of a Feminist And a Mad Scientist',2006,1,6,0.99,86,20.99,'PG'),
    ('ACE GOLDFINGER','Astounding Epistle of a Database Administrator',2006,1,3,4.99,48,12.99,'G'),
    ('ADAPTATION HOLES','Astounding Reflection of a Lumberjack',2006,1,7,2.99,50,18.99,'NC-17'),
    ('AFFAIR PREJUDICE','Fanciful Documentary of a Frisbee',2006,1,5,2.99,117,26.99,'G'),
    ('AFRICAN EGG','Incredible Drama of a Mad Scientist',2006,1,6,2.99,130,22.99,'G'),
    ('AGENT TRUMAN','Intrepid Panorama of a Robot',2006,1,3,2.99,169,17.99,'PG'),
    ('AIRPLANE SIERRA','Tasteful Panorama of a Feminist',2006,1,6,4.99,62,28.99,'PG-13'),
    ('AIRPORT POLLOCK','Brilliant Tale of a Explorer',2006,1,6,4.99,54,15.99,'R'),
    ('ALABAMA DEVIL','Thoughtful Panorama of a Database Administrator',2006,1,3,2.99,114,21.99,'PG-13'),
    ('ALADDIN CALENDAR','Drama of a Feminist And a Hunter',2006,1,6,4.99,63,29.99,'NC-17'),
    ('ALAMO VIDEOTAPE','Boring Epistle of a Butler',2006,1,6,0.99,126,16.99,'G'),
    ('ALASKA PHANTOM','Fanciful Satire of a Hunter',2006,1,6,0.99,136,22.99,'PG'),
    ('ALI FOREVER','Amazing Slice of Life with a Boat',2006,1,4,4.99,150,21.99,'PG'),
    ('ALICE FANTASIA','Fantasy of a Technical Writer',2006,1,6,0.99,94,23.99,'NC-17'),
    ('ALIEN CENTER','Brilliant Drama of a Cat',2006,1,6,2.99,46,10.99,'NC-17'),
    ('ALLEY EVOLUTION','Astounding Drama of a Composer',2006,1,6,2.99,180,23.99,'NC-17'),
    ('ALONE TRIP','Close-Encounters Performance of a Composer',2006,1,3,0.99,82,14.99,'R'),
    ('ALTER VICTORY','Thoughtful Drama of a Composer',2006,1,6,0.99,57,27.99,'PG-13'),
    ('AMADEUS HOLY','Explosive Punk film with a Secret Agent',2006,1,6,0.99,113,20.99,'PG'),
    ('AMELIE HELLFIGHTERS','Drama of a Woman And a Squirrel',2006,1,4,4.99,79,23.99,'R');

INSERT INTO inventory.film_actor (actor_id, film_id) VALUES
    (1,1),(1,2),(2,3),(2,4),(3,5),(3,6),(4,7),(4,8),(5,9),(5,10),
    (6,11),(6,12),(7,13),(7,14),(8,15),(8,16),(9,17),(9,18),(10,19),(10,20),
    (11,1),(12,2),(13,3),(14,4),(15,5),(16,6),(17,7),(18,8),(19,9),(20,10);

INSERT INTO inventory.film_category (film_id, category_id) VALUES
    (1,6),(2,11),(3,6),(4,11),(5,8),(6,6),(7,5),(8,11),(9,11),(10,9),
    (11,8),(12,9),(13,5),(14,7),(15,9),(16,9),(17,9),(18,11),(19,3),(20,5);

INSERT INTO sales.address (address, city, country, postal_code, phone) VALUES
    ('47 MySakila Drive','Aurora','Australia','',''),
    ('28 MySQL Boulevard','Woodridge','Australia','','14033335568'),
    ('23 Workhaven Lane','Lethbridge','Canada','','14033335568'),
    ('1411 Lillydale Drive','Woodridge','Australia','','6172235589'),
    ('1913 Hanoi Way','Sasebo','Japan','35200','28303384290'),
    ('1121 Loja Avenue','San Bernardino','United States','17886','838635286'),
    ('692 Joliet Street','Athenai','Greece','83579','448477190'),
    ('1294 Fianarantsoa Court','Kragujevac','Yugoslavia','',''),
    ('89 Namur Place','Brescia','Italy','','904253967'),
    ('676 Jarvis Street','Akishima','Japan','','813259425');

INSERT INTO sales.customer (store_id, first_name, last_name, email, address_id, active) VALUES
    (1,'MARY','SMITH','MARY.SMITH@sakilacustomer.org',3,true),
    (1,'PATRICIA','JOHNSON','PATRICIA.JOHNSON@sakilacustomer.org',4,true),
    (1,'LINDA','WILLIAMS','LINDA.WILLIAMS@sakilacustomer.org',5,true),
    (2,'BARBARA','JONES','BARBARA.JONES@sakilacustomer.org',6,true),
    (2,'ELIZABETH','BROWN','ELIZABETH.BROWN@sakilacustomer.org',7,true),
    (1,'JENNIFER','DAVIS','JENNIFER.DAVIS@sakilacustomer.org',8,true),
    (1,'MARIA','MILLER','MARIA.MILLER@sakilacustomer.org',9,true),
    (2,'SUSAN','WILSON','SUSAN.WILSON@sakilacustomer.org',10,true),
    (2,'MARGARET','MOORE','MARGARET.MOORE@sakilacustomer.org',3,true),
    (1,'DOROTHY','TAYLOR','DOROTHY.TAYLOR@sakilacustomer.org',4,false);

INSERT INTO staff.store (address_id) VALUES (1), (2);

INSERT INTO inventory.store_inventory (film_id, store_id) VALUES
    (1,1),(1,2),(2,1),(3,2),(4,1),(5,1),(6,2),(7,1),(8,2),(9,1),
    (10,2),(11,1),(12,2),(13,1),(14,2),(15,1),(16,2),(17,1),(18,2),(19,1);

INSERT INTO sales.rental (rental_date, inventory_id, customer_id, return_date, staff_id) VALUES
    ('2005-05-24 22:53:30',1,1,'2005-05-26 22:04:30',2),
    ('2005-05-24 22:54:33',2,2,'2005-05-28 19:40:33',1),
    ('2005-05-24 23:03:39',3,3,'2005-06-01 22:12:39',1),
    ('2005-05-24 23:04:41',4,4,'2005-06-03 01:43:41',2),
    ('2005-05-24 23:05:21',5,5,'2005-06-02 04:33:21',1),
    ('2005-05-24 23:08:07',6,6,'2005-05-27 01:32:07',1),
    ('2005-05-24 23:11:53',7,7,'2005-05-29 20:34:53',2),
    ('2005-05-24 23:31:46',8,8,'2005-05-27 23:33:46',2),
    ('2005-05-25 00:00:40',9,9,'2005-05-31 18:26:40',2),
    ('2005-05-25 00:02:21',10,10,'2005-06-03 07:53:21',1);

INSERT INTO sales.payment (customer_id, staff_id, rental_id, amount, payment_date) VALUES
    (1,1,1,2.99,'2005-05-25 11:30:37'),
    (2,1,2,0.99,'2005-05-28 10:35:23'),
    (3,2,3,5.99,'2005-06-15 00:54:12'),
    (4,2,4,0.99,'2005-06-15 18:02:53'),
    (5,2,5,9.99,'2005-06-15 21:08:46'),
    (6,1,6,4.99,'2005-06-16 15:18:57'),
    (7,2,7,4.99,'2005-06-17 02:50:51'),
    (8,2,8,0.99,'2005-06-17 09:38:22'),
    (9,2,9,5.99,'2005-06-17 16:40:33'),
    (10,1,10,4.99,'2005-06-18 08:46:50');
