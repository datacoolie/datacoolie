-- Sakila — MySQL's official DVD rental sample database (simplified schema+data)
-- Source: https://dev.mysql.com/doc/sakila/en/
-- schema+data only (no stored procedures or triggers)

-- ============================================================================
-- Drop and recreate sakila DB
-- ============================================================================
CREATE DATABASE IF NOT EXISTS sakila;
USE sakila;

-- ============================================================================
-- TABLES
-- ============================================================================

CREATE TABLE language (
    language_id  TINYINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    name         CHAR(20)         NOT NULL,
    last_update  TIMESTAMP        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB;

CREATE TABLE category (
    category_id  TINYINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    name         VARCHAR(25)      NOT NULL,
    last_update  TIMESTAMP        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB;

CREATE TABLE actor (
    actor_id    SMALLINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    first_name  VARCHAR(45)       NOT NULL,
    last_name   VARCHAR(45)       NOT NULL,
    last_update TIMESTAMP         NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    KEY idx_actor_last_name (last_name)
) ENGINE=InnoDB;

CREATE TABLE film (
    film_id           SMALLINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    title             VARCHAR(255)      NOT NULL,
    description       TEXT,
    release_year      YEAR,
    language_id       TINYINT UNSIGNED  NOT NULL,
    rental_duration   TINYINT UNSIGNED  NOT NULL DEFAULT 3,
    rental_rate       DECIMAL(4,2)      NOT NULL DEFAULT 4.99,
    length            SMALLINT UNSIGNED,
    replacement_cost  DECIMAL(5,2)      NOT NULL DEFAULT 19.99,
    rating            ENUM('G','PG','PG-13','R','NC-17') DEFAULT 'G',
    last_update       TIMESTAMP         NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    CONSTRAINT fk_film_language FOREIGN KEY (language_id) REFERENCES language(language_id),
    KEY idx_title (title)
) ENGINE=InnoDB;

CREATE TABLE film_actor (
    actor_id    SMALLINT UNSIGNED NOT NULL,
    film_id     SMALLINT UNSIGNED NOT NULL,
    last_update TIMESTAMP         NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (actor_id, film_id),
    CONSTRAINT fk_film_actor_actor FOREIGN KEY (actor_id) REFERENCES actor(actor_id),
    CONSTRAINT fk_film_actor_film  FOREIGN KEY (film_id)  REFERENCES film(film_id),
    KEY idx_fk_film_id (film_id)
) ENGINE=InnoDB;

CREATE TABLE film_category (
    film_id     SMALLINT UNSIGNED NOT NULL,
    category_id TINYINT UNSIGNED  NOT NULL,
    last_update TIMESTAMP         NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (film_id, category_id),
    CONSTRAINT fk_film_category_film     FOREIGN KEY (film_id)     REFERENCES film(film_id),
    CONSTRAINT fk_film_category_category FOREIGN KEY (category_id) REFERENCES category(category_id)
) ENGINE=InnoDB;

CREATE TABLE address (
    address_id   SMALLINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    address      VARCHAR(50)       NOT NULL,
    address2     VARCHAR(50)       DEFAULT NULL,
    district     VARCHAR(20)       NOT NULL,
    city         VARCHAR(50)       NOT NULL,
    country      VARCHAR(50)       NOT NULL,
    postal_code  VARCHAR(10)       DEFAULT NULL,
    phone        VARCHAR(20)       NOT NULL,
    last_update  TIMESTAMP         NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    KEY idx_fk_city (city)
) ENGINE=InnoDB;

CREATE TABLE customer (
    customer_id  SMALLINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    store_id     TINYINT UNSIGNED  NOT NULL,
    first_name   VARCHAR(45)       NOT NULL,
    last_name    VARCHAR(45)       NOT NULL,
    email        VARCHAR(50)       DEFAULT NULL,
    address_id   SMALLINT UNSIGNED NOT NULL,
    active       TINYINT(1)        NOT NULL DEFAULT 1,
    create_date  DATETIME          NOT NULL,
    last_update  TIMESTAMP         NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    CONSTRAINT fk_customer_address FOREIGN KEY (address_id) REFERENCES address(address_id),
    KEY idx_last_name (last_name),
    KEY idx_fk_store_id (store_id),
    KEY idx_fk_address_id (address_id)
) ENGINE=InnoDB;

CREATE TABLE store (
    store_id      TINYINT UNSIGNED  NOT NULL AUTO_INCREMENT PRIMARY KEY,
    manager_staff_id TINYINT UNSIGNED NOT NULL,
    address_id    SMALLINT UNSIGNED NOT NULL,
    last_update   TIMESTAMP         NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    CONSTRAINT fk_store_address FOREIGN KEY (address_id) REFERENCES address(address_id),
    KEY idx_fk_address_id (address_id)
) ENGINE=InnoDB;

CREATE TABLE inventory (
    inventory_id MEDIUMINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    film_id      SMALLINT UNSIGNED  NOT NULL,
    store_id     TINYINT UNSIGNED   NOT NULL,
    last_update  TIMESTAMP          NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    CONSTRAINT fk_inventory_film  FOREIGN KEY (film_id)  REFERENCES film(film_id),
    CONSTRAINT fk_inventory_store FOREIGN KEY (store_id) REFERENCES store(store_id),
    KEY idx_fk_film_id (film_id),
    KEY idx_store_id_film_id (store_id, film_id)
) ENGINE=InnoDB;

CREATE TABLE rental (
    rental_id    INT               NOT NULL AUTO_INCREMENT PRIMARY KEY,
    rental_date  DATETIME          NOT NULL,
    inventory_id MEDIUMINT UNSIGNED NOT NULL,
    customer_id  SMALLINT UNSIGNED NOT NULL,
    return_date  DATETIME          DEFAULT NULL,
    staff_id     TINYINT UNSIGNED  NOT NULL,
    last_update  TIMESTAMP         NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    CONSTRAINT fk_rental_inventory FOREIGN KEY (inventory_id) REFERENCES inventory(inventory_id),
    CONSTRAINT fk_rental_customer  FOREIGN KEY (customer_id)  REFERENCES customer(customer_id),
    UNIQUE KEY uq_rental_date (rental_date, inventory_id, customer_id),
    KEY idx_fk_inventory_id (inventory_id),
    KEY idx_fk_customer_id (customer_id)
) ENGINE=InnoDB;

CREATE TABLE payment (
    payment_id   SMALLINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    customer_id  SMALLINT UNSIGNED NOT NULL,
    staff_id     TINYINT UNSIGNED  NOT NULL,
    rental_id    INT               DEFAULT NULL,
    amount       DECIMAL(5,2)      NOT NULL,
    payment_date DATETIME          NOT NULL,
    last_update  TIMESTAMP         NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    CONSTRAINT fk_payment_customer FOREIGN KEY (customer_id) REFERENCES customer(customer_id),
    CONSTRAINT fk_payment_rental   FOREIGN KEY (rental_id)   REFERENCES rental(rental_id),
    KEY idx_fk_customer_id (customer_id),
    KEY fk_payment_rental (rental_id)
) ENGINE=InnoDB;

-- ============================================================================
-- VIEWS
-- ============================================================================

CREATE VIEW customer_list AS
SELECT
    cu.customer_id AS ID,
    CONCAT(cu.first_name,' ',cu.last_name) AS name,
    a.address,
    a.postal_code AS `zip code`,
    a.phone,
    a.city,
    a.country,
    IF(cu.active,'active','') AS notes,
    cu.store_id AS SID
FROM customer cu
JOIN address a ON cu.address_id = a.address_id;

CREATE VIEW film_list AS
SELECT
    f.film_id      AS FID,
    f.title        AS title,
    f.description  AS description,
    c.name         AS category,
    f.rental_rate  AS price,
    f.length       AS length,
    f.rating       AS rating,
    GROUP_CONCAT(CONCAT(a.first_name,' ',a.last_name) SEPARATOR ', ') AS actors
FROM film f
JOIN film_category fc ON f.film_id = fc.film_id
JOIN category c       ON fc.category_id = c.category_id
JOIN film_actor fa    ON f.film_id = fa.film_id
JOIN actor a          ON fa.actor_id = a.actor_id
GROUP BY f.film_id, f.title, f.description, c.name, f.rental_rate, f.length, f.rating;

CREATE VIEW sales_by_store AS
SELECT
    s.store_id,
    SUM(p.amount) AS total_sales
FROM store s
JOIN customer c  ON c.store_id = s.store_id
JOIN payment p   ON p.customer_id = c.customer_id
GROUP BY s.store_id;

-- ============================================================================
-- SEED DATA
-- ============================================================================

INSERT INTO language (name) VALUES ('English'),('Italian'),('Japanese'),('Mandarin'),('French'),('German');

INSERT INTO category (name) VALUES
    ('Action'),('Animation'),('Children'),('Classics'),('Comedy'),
    ('Documentary'),('Drama'),('Family'),('Foreign'),('Games'),
    ('Horror'),('Music'),('New'),('Sci-Fi'),('Sports'),('Travel');

INSERT INTO actor (first_name, last_name) VALUES
    ('PENELOPE','GUINESS'),('NICK','WAHLBERG'),('ED','CHASE'),
    ('JENNIFER','DAVIS'),('JOHNNY','LOLLOBRIGIDA'),('BETTE','NICHOLSON'),
    ('GRACE','MOSTEL'),('MATTHEW','JOHANSSON'),('JOE','SWANK'),
    ('CHRISTIAN','GABLE'),('ZERO','CAGE'),('KARL','BERRY'),
    ('UMA','WOOD'),('VIVIEN','BERGEN'),('CUBA','OLIVIER'),
    ('FRED','COSTNER'),('HELEN','VOIGHT'),('DAN','TORN'),
    ('BOB','FAWCETT'),('LUCILLE','TRACY');

INSERT INTO film (title, description, release_year, language_id, rental_duration, rental_rate, length, replacement_cost, rating) VALUES
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

INSERT INTO film_actor (actor_id, film_id) VALUES
    (1,1),(1,5),(2,3),(2,4),(3,5),(3,6),(4,7),(4,8),(5,9),(5,10),
    (6,11),(6,12),(7,13),(7,14),(8,15),(8,16),(9,17),(9,18),(10,19),(10,20);

INSERT INTO film_category (film_id, category_id) VALUES
    (1,6),(2,11),(3,6),(4,11),(5,8),(6,6),(7,5),(8,11),(9,11),(10,9),
    (11,8),(12,9),(13,5),(14,7),(15,9),(16,9),(17,9),(18,11),(19,3),(20,5);

INSERT INTO address (address, district, city, country, postal_code, phone) VALUES
    ('47 MySakila Drive','Alberta','Aurora','Australia','',''),
    ('28 MySQL Boulevard','QLD','Woodridge','Australia','','14033335568'),
    ('23 Workhaven Lane','Alberta','Lethbridge','Canada','','14033335568'),
    ('1411 Lillydale Drive','QLD','Woodridge','Australia','','6172235589'),
    ('1913 Hanoi Way','Nagasaki','Sasebo','Japan','35200','28303384290'),
    ('1121 Loja Avenue','California','San Bernardino','United States','17886','838635286');

INSERT INTO store (manager_staff_id, address_id) VALUES (1,1),(2,2);

INSERT INTO customer (store_id, first_name, last_name, email, address_id, active, create_date) VALUES
    (1,'MARY','SMITH','MARY.SMITH@sakilacustomer.org',3,1,'2006-02-14 22:04:36'),
    (1,'PATRICIA','JOHNSON','PATRICIA.JOHNSON@sakilacustomer.org',4,1,'2006-02-14 22:04:36'),
    (1,'LINDA','WILLIAMS','LINDA.WILLIAMS@sakilacustomer.org',5,1,'2006-02-14 22:04:36'),
    (2,'BARBARA','JONES','BARBARA.JONES@sakilacustomer.org',6,1,'2006-02-14 22:04:36'),
    (2,'ELIZABETH','BROWN','ELIZABETH.BROWN@sakilacustomer.org',3,1,'2006-02-14 22:04:36'),
    (1,'JENNIFER','DAVIS','JENNIFER.DAVIS@sakilacustomer.org',4,1,'2006-02-14 22:04:36'),
    (1,'MARIA','MILLER','MARIA.MILLER@sakilacustomer.org',5,1,'2006-02-14 22:04:36'),
    (2,'SUSAN','WILSON','SUSAN.WILSON@sakilacustomer.org',6,1,'2006-02-14 22:04:36');

INSERT INTO inventory (film_id, store_id) VALUES
    (1,1),(1,2),(2,1),(3,2),(4,1),(5,1),(6,2),(7,1),(8,2),(9,1),
    (10,2),(11,1),(12,2),(13,1),(14,2),(15,1);

INSERT INTO rental (rental_date, inventory_id, customer_id, return_date, staff_id) VALUES
    ('2005-05-24 22:53:30',1,1,'2005-05-26 22:04:30',2),
    ('2005-05-24 22:54:33',2,2,'2005-05-28 19:40:33',1),
    ('2005-05-24 23:03:39',3,3,'2005-06-01 22:12:39',1),
    ('2005-05-24 23:04:41',4,4,'2005-06-03 01:43:41',2),
    ('2005-05-24 23:05:21',5,5,'2005-06-02 04:33:21',1),
    ('2005-05-24 23:08:07',6,6,'2005-05-27 01:32:07',1),
    ('2005-05-24 23:11:53',7,7,'2005-05-29 20:34:53',2),
    ('2005-05-24 23:31:46',8,8,'2005-05-27 23:33:46',2);

INSERT INTO payment (customer_id, staff_id, rental_id, amount, payment_date) VALUES
    (1,1,1,2.99,'2005-05-25 11:30:37'),
    (2,1,2,0.99,'2005-05-28 10:35:23'),
    (3,2,3,5.99,'2005-06-15 00:54:12'),
    (4,2,4,0.99,'2005-06-15 18:02:53'),
    (5,2,5,9.99,'2005-06-15 21:08:46'),
    (6,1,6,4.99,'2005-06-16 15:18:57'),
    (7,2,7,4.99,'2005-06-17 02:50:51'),
    (8,2,8,0.99,'2005-06-17 09:38:22');
