-- AdventureWorks LT — Microsoft's lightweight bicycle manufacturer sample database
-- Schema: SalesLT — 12 tables, FKs, indexes, views
-- Source: https://learn.microsoft.com/en-us/sql/samples/adventureworks-install-configure
-- Simplified to CREATE TABLE + INSERT (no stored procedures, no full-text indexes)

-- ============================================================================
-- CREATE DATABASE + SCHEMA
-- ============================================================================
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'AdventureWorksLT')
    CREATE DATABASE AdventureWorksLT;
GO

USE AdventureWorksLT;
GO

IF NOT EXISTS (SELECT schema_id FROM sys.schemas WHERE name = 'SalesLT')
    EXEC('CREATE SCHEMA SalesLT');
GO

-- ============================================================================
-- TABLES
-- ============================================================================

CREATE TABLE SalesLT.ErrorLog (
    ErrorLogID    INT          IDENTITY(1,1) NOT NULL CONSTRAINT PK_ErrorLog PRIMARY KEY,
    ErrorTime     DATETIME     NOT NULL CONSTRAINT DF_ErrorLog_ErrorTime DEFAULT (GETDATE()),
    UserName      NVARCHAR(128) NOT NULL,
    ErrorNumber   INT          NOT NULL,
    ErrorSeverity INT          NULL,
    ErrorState    INT          NULL,
    ErrorProcedure NVARCHAR(126) NULL,
    ErrorLine     INT          NULL,
    ErrorMessage  NVARCHAR(4000) NOT NULL
);
GO

CREATE TABLE SalesLT.BuildVersion (
    SystemInformationID TINYINT      NOT NULL CONSTRAINT PK_BuildVersion PRIMARY KEY,
    [Database Version]  NVARCHAR(25) NOT NULL,
    VersionDate         DATETIME     NOT NULL,
    ModifiedDate        DATETIME     NOT NULL CONSTRAINT DF_BuildVersion_ModifiedDate DEFAULT (GETDATE())
);
GO

CREATE TABLE SalesLT.Address (
    AddressID    INT           IDENTITY(1,1) NOT NULL CONSTRAINT PK_Address_AddressID PRIMARY KEY,
    AddressLine1 NVARCHAR(60)  NOT NULL,
    AddressLine2 NVARCHAR(60)  NULL,
    City         NVARCHAR(30)  NOT NULL,
    StateProvince NVARCHAR(50) NOT NULL,
    CountryRegion NVARCHAR(50) NOT NULL,
    PostalCode   NVARCHAR(15)  NOT NULL,
    rowguid      UNIQUEIDENTIFIER NOT NULL CONSTRAINT DF_Address_rowguid DEFAULT (NEWID()),
    ModifiedDate DATETIME      NOT NULL CONSTRAINT DF_Address_ModifiedDate DEFAULT (GETDATE()),
    CONSTRAINT AK_Address_rowguid UNIQUE (rowguid)
);
GO
CREATE INDEX IX_Address_AddressLine1_AddressLine2_City_StateProvince_PostalCode_CountryRegion
    ON SalesLT.Address (AddressLine1, AddressLine2, City, StateProvince, PostalCode, CountryRegion);
GO

CREATE TABLE SalesLT.Customer (
    CustomerID     INT           IDENTITY(1,1) NOT NULL CONSTRAINT PK_Customer_CustomerID PRIMARY KEY,
    NameStyle      BIT           NOT NULL CONSTRAINT DF_Customer_NameStyle DEFAULT (0),
    Title          NVARCHAR(8)   NULL,
    FirstName      NVARCHAR(50)  NOT NULL,
    MiddleName     NVARCHAR(50)  NULL,
    LastName       NVARCHAR(50)  NOT NULL,
    Suffix         NVARCHAR(10)  NULL,
    CompanyName    NVARCHAR(128) NULL,
    SalesPerson    NVARCHAR(256) NULL,
    EmailAddress   NVARCHAR(50)  NULL,
    Phone          NVARCHAR(25)  NULL,
    PasswordHash   VARCHAR(128)  NOT NULL,
    PasswordSalt   VARCHAR(10)   NOT NULL,
    rowguid        UNIQUEIDENTIFIER NOT NULL CONSTRAINT DF_Customer_rowguid DEFAULT (NEWID()),
    ModifiedDate   DATETIME      NOT NULL CONSTRAINT DF_Customer_ModifiedDate DEFAULT (GETDATE()),
    CONSTRAINT AK_Customer_rowguid UNIQUE (rowguid)
);
GO
CREATE INDEX IX_Customer_EmailAddress ON SalesLT.Customer (EmailAddress);
GO

CREATE TABLE SalesLT.CustomerAddress (
    CustomerID   INT          NOT NULL,
    AddressID    INT          NOT NULL,
    AddressType  NVARCHAR(50) NOT NULL,
    rowguid      UNIQUEIDENTIFIER NOT NULL CONSTRAINT DF_CustomerAddress_rowguid DEFAULT (NEWID()),
    ModifiedDate DATETIME     NOT NULL CONSTRAINT DF_CustomerAddress_ModifiedDate DEFAULT (GETDATE()),
    CONSTRAINT PK_CustomerAddress_CustomerID_AddressID PRIMARY KEY (CustomerID, AddressID),
    CONSTRAINT FK_CustomerAddress_Customer_CustomerID FOREIGN KEY (CustomerID) REFERENCES SalesLT.Customer(CustomerID),
    CONSTRAINT FK_CustomerAddress_Address_AddressID   FOREIGN KEY (AddressID)  REFERENCES SalesLT.Address(AddressID)
);
GO

CREATE TABLE SalesLT.ProductCategory (
    ProductCategoryID       INT          IDENTITY(1,1) NOT NULL CONSTRAINT PK_ProductCategory_ProductCategoryID PRIMARY KEY,
    ParentProductCategoryID INT          NULL CONSTRAINT FK_ProductCategory_ProductCategory REFERENCES SalesLT.ProductCategory(ProductCategoryID),
    Name                    NVARCHAR(50) NOT NULL,
    rowguid                 UNIQUEIDENTIFIER NOT NULL CONSTRAINT DF_ProductCategory_rowguid DEFAULT (NEWID()),
    ModifiedDate            DATETIME     NOT NULL CONSTRAINT DF_ProductCategory_ModifiedDate DEFAULT (GETDATE()),
    CONSTRAINT AK_ProductCategory_Name    UNIQUE (Name),
    CONSTRAINT AK_ProductCategory_rowguid UNIQUE (rowguid)
);
GO

CREATE TABLE SalesLT.ProductModel (
    ProductModelID  INT            IDENTITY(1,1) NOT NULL CONSTRAINT PK_ProductModel_ProductModelID PRIMARY KEY,
    Name            NVARCHAR(50)   NOT NULL,
    CatalogDescription XML         NULL,
    rowguid         UNIQUEIDENTIFIER NOT NULL CONSTRAINT DF_ProductModel_rowguid DEFAULT (NEWID()),
    ModifiedDate    DATETIME       NOT NULL CONSTRAINT DF_ProductModel_ModifiedDate DEFAULT (GETDATE()),
    CONSTRAINT AK_ProductModel_Name    UNIQUE (Name),
    CONSTRAINT AK_ProductModel_rowguid UNIQUE (rowguid)
);
GO

CREATE TABLE SalesLT.ProductModelProductDescription (
    ProductModelID        INT         NOT NULL,
    ProductDescriptionID  INT         NOT NULL,
    Culture               NCHAR(6)    NOT NULL,
    rowguid               UNIQUEIDENTIFIER NOT NULL CONSTRAINT DF_ProductModelProductDescription_rowguid DEFAULT (NEWID()),
    ModifiedDate          DATETIME    NOT NULL CONSTRAINT DF_ProductModelProductDescription_ModifiedDate DEFAULT (GETDATE()),
    CONSTRAINT PK_ProductModelProductDescription_ProductModelID_ProductDescriptionID_Culture
        PRIMARY KEY (ProductModelID, ProductDescriptionID, Culture)
);
GO

CREATE TABLE SalesLT.ProductDescription (
    ProductDescriptionID INT           IDENTITY(1,1) NOT NULL CONSTRAINT PK_ProductDescription_ProductDescriptionID PRIMARY KEY,
    Description          NVARCHAR(400) NOT NULL,
    rowguid              UNIQUEIDENTIFIER NOT NULL CONSTRAINT DF_ProductDescription_rowguid DEFAULT (NEWID()),
    ModifiedDate         DATETIME      NOT NULL CONSTRAINT DF_ProductDescription_ModifiedDate DEFAULT (GETDATE()),
    CONSTRAINT AK_ProductDescription_rowguid UNIQUE (rowguid)
);
GO

ALTER TABLE SalesLT.ProductModelProductDescription
    ADD CONSTRAINT FK_ProductModelProductDescription_ProductModel
        FOREIGN KEY (ProductModelID) REFERENCES SalesLT.ProductModel(ProductModelID),
    CONSTRAINT FK_ProductModelProductDescription_ProductDescription
        FOREIGN KEY (ProductDescriptionID) REFERENCES SalesLT.ProductDescription(ProductDescriptionID);
GO

CREATE TABLE SalesLT.Product (
    ProductID            INT            IDENTITY(1,1) NOT NULL CONSTRAINT PK_Product_ProductID PRIMARY KEY,
    Name                 NVARCHAR(50)   NOT NULL,
    ProductNumber        NVARCHAR(25)   NOT NULL,
    Color                NVARCHAR(15)   NULL,
    StandardCost         MONEY          NOT NULL,
    ListPrice            MONEY          NOT NULL,
    Size                 NVARCHAR(5)    NULL,
    Weight               DECIMAL(8,2)   NULL,
    ProductCategoryID    INT            NULL CONSTRAINT FK_Product_ProductCategory REFERENCES SalesLT.ProductCategory(ProductCategoryID),
    ProductModelID       INT            NULL CONSTRAINT FK_Product_ProductModel     REFERENCES SalesLT.ProductModel(ProductModelID),
    SellStartDate        DATETIME       NOT NULL,
    SellEndDate          DATETIME       NULL,
    DiscontinuedDate     DATETIME       NULL,
    ThumbNailPhoto       VARBINARY(MAX) NULL,
    ThumbnailPhotoFileName NVARCHAR(50) NULL,
    rowguid              UNIQUEIDENTIFIER NOT NULL CONSTRAINT DF_Product_rowguid DEFAULT (NEWID()),
    ModifiedDate         DATETIME       NOT NULL CONSTRAINT DF_Product_ModifiedDate DEFAULT (GETDATE()),
    CONSTRAINT AK_Product_Name          UNIQUE (Name),
    CONSTRAINT AK_Product_ProductNumber UNIQUE (ProductNumber),
    CONSTRAINT AK_Product_rowguid       UNIQUE (rowguid)
);
GO
CREATE INDEX IX_Product_Name   ON SalesLT.Product (Name);
CREATE INDEX IX_Product_Color  ON SalesLT.Product (Color);
GO

CREATE TABLE SalesLT.SalesOrderHeader (
    SalesOrderID          INT            IDENTITY(1,1) NOT NULL CONSTRAINT PK_SalesOrderHeader_SalesOrderID PRIMARY KEY,
    RevisionNumber        TINYINT        NOT NULL CONSTRAINT DF_SalesOrderHeader_RevisionNumber DEFAULT (0),
    OrderDate             DATETIME       NOT NULL CONSTRAINT DF_SalesOrderHeader_OrderDate DEFAULT (GETDATE()),
    DueDate               DATETIME       NOT NULL,
    ShipDate              DATETIME       NULL,
    Status                TINYINT        NOT NULL CONSTRAINT DF_SalesOrderHeader_Status DEFAULT (1),
    OnlineOrderFlag       BIT            NOT NULL CONSTRAINT DF_SalesOrderHeader_OnlineOrderFlag DEFAULT (1),
    SalesOrderNumber      AS ISNULL(N'SO' + CONVERT(NVARCHAR(23), SalesOrderID), N'*** ERROR ***'),
    PurchaseOrderNumber   NVARCHAR(25)   NULL,
    AccountNumber         NVARCHAR(15)   NULL,
    CustomerID            INT            NOT NULL CONSTRAINT FK_SalesOrderHeader_Customer REFERENCES SalesLT.Customer(CustomerID),
    ShipToAddressID       INT            NULL CONSTRAINT FK_SalesOrderHeader_Address_ShipTo REFERENCES SalesLT.Address(AddressID),
    BillToAddressID       INT            NULL CONSTRAINT FK_SalesOrderHeader_Address_BillTo REFERENCES SalesLT.Address(AddressID),
    ShipMethod            NVARCHAR(50)   NOT NULL,
    CreditCardApprovalCode NVARCHAR(15)  NULL,
    SubTotal              MONEY          NOT NULL CONSTRAINT DF_SalesOrderHeader_SubTotal DEFAULT (0),
    TaxAmt                MONEY          NOT NULL CONSTRAINT DF_SalesOrderHeader_TaxAmt DEFAULT (0),
    Freight               MONEY          NOT NULL CONSTRAINT DF_SalesOrderHeader_Freight DEFAULT (0),
    TotalDue              AS ISNULL(SubTotal + TaxAmt + Freight, 0),
    Comment               NVARCHAR(MAX)  NULL,
    rowguid               UNIQUEIDENTIFIER NOT NULL CONSTRAINT DF_SalesOrderHeader_rowguid DEFAULT (NEWID()),
    ModifiedDate          DATETIME       NOT NULL CONSTRAINT DF_SalesOrderHeader_ModifiedDate DEFAULT (GETDATE())
);
GO
CREATE INDEX IX_SalesOrderHeader_CustomerID ON SalesLT.SalesOrderHeader (CustomerID);
GO

CREATE TABLE SalesLT.SalesOrderDetail (
    SalesOrderID        INT              NOT NULL,
    SalesOrderDetailID  INT              IDENTITY(1,1) NOT NULL,
    OrderQty            SMALLINT         NOT NULL,
    ProductID           INT              NOT NULL,
    UnitPrice           MONEY            NOT NULL,
    UnitPriceDiscount   MONEY            NOT NULL CONSTRAINT DF_SalesOrderDetail_UnitPriceDiscount DEFAULT (0),
    LineTotal           AS ISNULL(UnitPrice * (1.0 - UnitPriceDiscount) * OrderQty, 0),
    rowguid             UNIQUEIDENTIFIER NOT NULL CONSTRAINT DF_SalesOrderDetail_rowguid DEFAULT (NEWID()),
    ModifiedDate        DATETIME         NOT NULL CONSTRAINT DF_SalesOrderDetail_ModifiedDate DEFAULT (GETDATE()),
    CONSTRAINT PK_SalesOrderDetail_SalesOrderID_SalesOrderDetailID PRIMARY KEY (SalesOrderID, SalesOrderDetailID),
    CONSTRAINT FK_SalesOrderDetail_SalesOrderHeader FOREIGN KEY (SalesOrderID) REFERENCES SalesLT.SalesOrderHeader(SalesOrderID),
    CONSTRAINT FK_SalesOrderDetail_Product          FOREIGN KEY (ProductID)    REFERENCES SalesLT.Product(ProductID)
);
GO
CREATE INDEX IX_SalesOrderDetail_ProductID ON SalesLT.SalesOrderDetail (ProductID);
GO

-- ============================================================================
-- VIEWS
-- ============================================================================

CREATE VIEW SalesLT.vProductAndDescription AS
SELECT
    p.ProductID,
    p.Name,
    pm.Name  AS ProductModel,
    pmx.Culture,
    pd.Description
FROM SalesLT.Product p
JOIN SalesLT.ProductModel pm         ON p.ProductModelID = pm.ProductModelID
JOIN SalesLT.ProductModelProductDescription pmx ON pm.ProductModelID = pmx.ProductModelID
JOIN SalesLT.ProductDescription pd   ON pmx.ProductDescriptionID = pd.ProductDescriptionID;
GO

CREATE VIEW SalesLT.vGetAllCategories AS
SELECT
    pcat.ProductCategoryID       AS ParentProductCategoryID,
    pcat.Name                    AS ParentProductCategoryName,
    scat.ProductCategoryID,
    scat.Name                    AS ProductCategoryName
FROM SalesLT.ProductCategory scat
LEFT JOIN SalesLT.ProductCategory pcat ON scat.ParentProductCategoryID = pcat.ProductCategoryID;
GO

-- ============================================================================
-- SEED DATA
-- ============================================================================

INSERT INTO SalesLT.ProductCategory (Name, ParentProductCategoryID) VALUES
    ('Bikes',NULL),('Components',NULL),('Clothing',NULL),('Accessories',NULL),
    ('Mountain Bikes',1),('Road Bikes',1),('Touring Bikes',1),
    ('Handlebars',2),('Bottom Brackets',2),('Brakes',2),
    ('Chains',2),('Cranksets',2),('Derailleurs',2);
GO

INSERT INTO SalesLT.ProductModel (Name) VALUES
    ('Classic Vest'),('Full-Finger Gloves'),('Half-Finger Gloves'),
    ('HL Road Frame'),('LL Road Frame'),('ML Road Frame'),
    ('Mountain-100'),('Mountain-200'),('Road-150'),('Road-250');
GO

INSERT INTO SalesLT.ProductDescription (Description) VALUES
    ('Lightweight and durable.'),
    ('Aerodynamic design for maximum speed.'),
    ('Comfortable ride in all conditions.'),
    ('Classic mountain frame, approved for all-terrain use.'),
    ('Entry-level road frame with aluminum construction.');
GO

INSERT INTO SalesLT.Product (Name, ProductNumber, Color, StandardCost, ListPrice, Size, Weight, ProductCategoryID, ProductModelID, SellStartDate) VALUES
    ('HL Road Frame - Black, 58','FR-R92B-58','Black',1431.50,1431.50,'58',1016.04,6,4,'1998-06-01'),
    ('HL Road Frame - Red, 58','FR-R92R-58','Red',1431.50,1431.50,'58',1016.04,6,4,'1998-06-01'),
    ('Sport-100 Helmet, Red','HL-U509-R','Red',13.09,34.99,NULL,NULL,4,NULL,'2005-07-01'),
    ('Sport-100 Helmet, Black','HL-U509','Black',13.09,34.99,NULL,NULL,4,NULL,'2005-07-01'),
    ('Mountain Bike Socks, M','SO-B909-M','White',3.39,9.50,'M',NULL,3,NULL,'2005-07-01'),
    ('Mountain Bike Socks, L','SO-B909-L','White',3.39,9.50,'L',NULL,3,NULL,'2005-07-01'),
    ('AWC Logo Cap','CA-1098',NULL,6.92,8.99,NULL,NULL,4,NULL,'2000-06-01'),
    ('Long-Sleeve Logo Jersey, S','LJ-0192-S','Multi',38.49,49.99,'S',NULL,3,NULL,'2000-06-01'),
    ('Long-Sleeve Logo Jersey, M','LJ-0192-M','Multi',38.49,49.99,'M',NULL,3,NULL,'2000-06-01'),
    ('Long-Sleeve Logo Jersey, L','LJ-0192-L','Multi',38.49,49.99,'L',NULL,3,NULL,'2000-06-01');
GO

INSERT INTO SalesLT.Address (AddressLine1, City, StateProvince, CountryRegion, PostalCode) VALUES
    ('1970 Napa Ct.','Bothell','Washington','United States','98011'),
    ('9833 Mt. Dias Blv.','Bothell','Washington','United States','98011'),
    ('7484 Roundtree Drive','Bothell','Washington','United States','98011'),
    ('9539 Glenside Dr','Bothell','Washington','United States','98011'),
    ('1226 Shoe St.','Bothell','Washington','United States','98011');
GO

INSERT INTO SalesLT.Customer (NameStyle, Title, FirstName, LastName, CompanyName, EmailAddress, Phone, PasswordHash, PasswordSalt) VALUES
    (0,'Mr.','Orlando','Gee','A Bike Store','orlando0@adventure-works.com','245-555-0173','L/Rlwxzp4w7RWmEgXX+/A7cXaePEPcp+KwQhl2fJL7w=','1KjXYs4='),
    (0,'Mr.','Keith','Harris','Progressive Sports','keith0@adventure-works.com','170-555-0127','YPdtRdvqeAhj6wyxEsFdshBDNXxkCXn+CRgbvJItknw=','fs1ZGhY='),
    (0,'Ms.','Donna','Carreras','A Bike Store','donna0@adventure-works.com','279-555-0130','LNoK27abGQo48gGue3EBV/UrlYSToV0/s87dCRV7uJk=','YTNH5Rw='),
    (0,'Ms.','Janet','Gates','Modular Cycle Systems','janet1@adventure-works.com','710-555-0173','ElzTpSNbUW1Ut+L5cWlfR7MF6nBZia8WpmGaQPjLOJA=','ZdgG='),
    (0,'Mr.','Lucy','Harrington','Metropolitan Sports Supply','lucy0@adventure-works.com','828-555-0186','KJqV15wsX3PG8sFNb4OiY8DmPIGLAHYpYSPsNBY5cTc=','Cv7La2g=');
GO

INSERT INTO SalesLT.CustomerAddress (CustomerID, AddressID, AddressType) VALUES
    (1,1,'Main Office'),(2,2,'Main Office'),(3,3,'Main Office'),
    (4,4,'Main Office'),(5,5,'Main Office');
GO

INSERT INTO SalesLT.SalesOrderHeader (OrderDate, DueDate, ShipDate, Status, CustomerID, ShipToAddressID, BillToAddressID, ShipMethod, SubTotal, TaxAmt, Freight) VALUES
    ('2008-06-01','2008-06-13','2008-06-08',5,1,1,1,'CARGO TRANSPORT 5',1000.00,80.00,50.00),
    ('2008-06-01','2008-06-13','2008-06-08',5,2,2,2,'CARGO TRANSPORT 5',2000.00,160.00,100.00),
    ('2008-07-01','2008-07-13','2008-07-08',5,3,3,3,'CARGO TRANSPORT 5',1500.00,120.00,75.00),
    ('2008-07-15','2008-07-27',NULL,1,4,4,4,'CARGO TRANSPORT 5',3500.00,280.00,175.00);
GO

INSERT INTO SalesLT.SalesOrderDetail (SalesOrderID, OrderQty, ProductID, UnitPrice, UnitPriceDiscount) VALUES
    (1,1,1,1431.50,0.00),
    (1,2,3,34.99,0.10),
    (2,1,2,1431.50,0.00),
    (2,1,4,34.99,0.00),
    (3,3,5,9.50,0.00),
    (3,1,7,8.99,0.00),
    (4,2,8,49.99,0.05),
    (4,1,9,49.99,0.05),
    (4,1,10,49.99,0.05);
GO
