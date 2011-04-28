-- -------------------------------------------------------------------------------
--  Schema definition for the TransformationDB database a generic
--  engine to define input data streams and support dynamic data 
--  grouping per unit of execution.

DROP DATABASE IF EXISTS ProcessDB;
CREATE DATABASE ProcessDB;
-- ------------------------------------------------------------------------------

-- Database owner definition
USE mysql;

-- Must set passwords for database user by replacing "must_be_set".
GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON ProcessDB.* TO Dirac@'%' IDENTIFIED BY 'must_be_set';
FLUSH PRIVILEGES;

use ProcessDB;
-- -----------------------------------------------------
-- Table Processes
-- -----------------------------------------------------
DROP TABLE IF EXISTS Processes ;

CREATE  TABLE IF NOT EXISTS Processes (
  idProcesses INT NOT NULL AUTO_INCREMENT ,
  ProcessName VARCHAR(45) NOT NULL ,
  Detail VARCHAR(45) NULL ,
  PRIMARY KEY (idProcesses) ,
  INDEX ProcessName (ProcessName ASC) ,
  UNIQUE INDEX idProcesses_UNIQUE (idProcesses ASC) ,
  UNIQUE INDEX ProcessName_UNIQUE (ProcessName ASC) )
ENGINE = MyISAM
AUTO_INCREMENT = 300000;


-- -----------------------------------------------------
-- Table Software
-- -----------------------------------------------------
DROP TABLE IF EXISTS Software ;

CREATE  TABLE IF NOT EXISTS Software (
  idSoftware INT NOT NULL AUTO_INCREMENT ,
  AppName VARCHAR(45) NOT NULL ,
  AppVersion VARCHAR(45) NOT NULL ,
  Platform   VARCHAR(45) NOT NULL ,
  Valid TINYINT(1)  NOT NULL DEFAULT TRUE ,
  `Comment` VARCHAR(255) NULL ,
  `UpdateComment` VARCHAR(255) NULL ,
  Defined DATETIME ,
  LastUpdate DATETIME ,
  Path VARCHAR(512) NOT NULL,
  PRIMARY KEY (idSoftware) ,
  INDEX Application (AppName ASC, AppVersion ASC) ,
  UNIQUE INDEX idSoftware_UNIQUE (idSoftware ASC) )
ENGINE = MyISAM;


-- -----------------------------------------------------
-- Table ProcessDB.Processes_has_Software
-- -----------------------------------------------------
DROP TABLE IF EXISTS Processes_has_Software ;

CREATE  TABLE IF NOT EXISTS Processes_has_Software (
  idProcesses INT NOT NULL ,
  idSoftware INT NOT NULL ,
  Template VARCHAR(45) NULL ,
  PRIMARY KEY (idProcesses, idSoftware) ,
  INDEX fk_Processes_has_Software_Software1 (idSoftware ASC) ,
  INDEX fk_Processes_has_Software_Processes1 (idProcesses ASC) ,
  CONSTRAINT fk_Processes_has_Software_Processes1
    FOREIGN KEY (idProcesses )
    REFERENCES Processes (idProcesses )
    ON DELETE SET NULL
    ON UPDATE NO ACTION,
  CONSTRAINT fk_Processes_has_Software_Software1
    FOREIGN KEY (idSoftware )
    REFERENCES Software (idSoftware )
    ON DELETE SET NULL
    ON UPDATE NO ACTION)
ENGINE = MyISAM;


-- -----------------------------------------------------
-- Table ProcessDB.ProcessData
-- -----------------------------------------------------
DROP TABLE IF EXISTS ProcessData ;

CREATE  TABLE IF NOT EXISTS ProcessData (
  CrossSection DOUBLE(10,6) NULL DEFAULT 0 ,
  Path VARCHAR(255) NULL ,
  NbEvts INT NULL DEFAULT 0 ,
  Files INT NULL DEFAULT 0 ,
  idProcessData INT NOT NULL AUTO_INCREMENT ,
  idProcesses INT NOT NULL ,
  Polarisation VARCHAR(10) NULL ,
  PRIMARY KEY (idProcessData, idProcesses) ,
  INDEX fk_ProcessData_Processes1 (idProcesses ASC) ,
  UNIQUE INDEX idProcessData_UNIQUE (idProcessData ASC) ,
  CONSTRAINT fk_ProcessData_Processes1
    FOREIGN KEY (idProcesses )
    REFERENCES Processes (idProcesses )
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = MyISAM;


-- -----------------------------------------------------
-- Table ProcessDB.Productions
-- -----------------------------------------------------
DROP TABLE IF EXISTS Productions ;

CREATE  TABLE IF NOT EXISTS Productions (
  idSoftware INT NOT NULL ,
  idProcessData INT NOT NULL ,
  ProdID INT NOT NULL ,
  ProdDetail VARCHAR(255) BINARY NULL ,
  idProductions INT NOT NULL AUTO_INCREMENT ,
  Type VARCHAR(45) NOT NULL ,
  PRIMARY KEY (idProductions, idProcessData) ,
  INDEX fk_Software_has_ProcessData_Software1 (idSoftware ASC) ,
  INDEX fk_Productions_ProcessData1 (idProcessData ASC) ,
  INDEX ProdID (ProdID ASC) ,
  CONSTRAINT fk_Software_has_ProcessData_Software1
    FOREIGN KEY (idSoftware )
    REFERENCES Software (idSoftware )
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT fk_Productions_ProcessData1
    FOREIGN KEY (idProcessData )
    REFERENCES ProcessData (idProcessData )
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = MyISAM;


-- -----------------------------------------------------
-- Table ProcessDB.SteeringFiles
-- -----------------------------------------------------
DROP TABLE IF EXISTS SteeringFiles ;

CREATE  TABLE IF NOT EXISTS ProcessDB.SteeringFiles (
  idfiles INT NOT NULL AUTO_INCREMENT ,
  FileName VARCHAR(45) NOT NULL ,
  Path VARCHAR(255) NOT NULL ,
  PRIMARY KEY (idfiles, FileName) ,
  UNIQUE INDEX FileName_UNIQUE (FileName ASC) )
ENGINE = MyISAM;


-- -----------------------------------------------------
-- Table ProcessDB.SteeringFiles_has_ProcessData
-- -----------------------------------------------------
DROP TABLE IF EXISTS SteeringFiles_has_ProcessData ;

CREATE  TABLE IF NOT EXISTS SteeringFiles_has_ProcessData (
  idfiles INT NOT NULL ,
  idProcessData INT NOT NULL ,
  PRIMARY KEY (idfiles, idProcessData) ,
  INDEX fk_SteeringFiles_has_ProcessData_ProcessData1 (idProcessData ASC) ,
  INDEX fk_SteeringFiles_has_ProcessData_SteeringFiles1 (idfiles ASC) ,
  CONSTRAINT fk_SteeringFiles_has_ProcessData_SteeringFiles1
    FOREIGN KEY (idfiles )
    REFERENCES SteeringFiles (idfiles )
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT fk_SteeringFiles_has_ProcessData_ProcessData1
    FOREIGN KEY (idProcessData )
    REFERENCES ProcessData (idProcessData )
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = MyISAM;


-- -----------------------------------------------------
-- Table ProcessDB.DependencyRelation
-- -----------------------------------------------------
DROP TABLE IF EXISTS `DependencyRelation` ;

CREATE  TABLE IF NOT EXISTS `DependencyRelation` (
  `idSoftware` INT NOT NULL ,
  `idDependency` INT NOT NULL ,
  `idDependencyRelation` INT NOT NULL AUTO_INCREMENT ,
  PRIMARY KEY (`idDependencyRelation`) ,
  INDEX `fk_Software_has_Software_Software2` (`idDependency` ASC) ,
  INDEX `fk_Software_has_Software_Software1` (`idSoftware` ASC) ,
  CONSTRAINT `fk_Software_has_Software_Software1`
    FOREIGN KEY (`idSoftware` )
    REFERENCES `Software` (`idSoftware` )
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_Software_has_Software_Software2`
    FOREIGN KEY (`idDependency` )
    REFERENCES `Software` (`idSoftware` )
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = MyISAM;

-- -----------------------------------------------------
-- Table ProcessDB.ProductionRelation
-- -----------------------------------------------------
DROP TABLE IF EXISTS `ProductionRelation` ;

CREATE TABLE IF NOT EXISTS `ProductionRelation` (
  idRelation INT NOT NULL AUTO_INCREMENT,
  idMotherProd INT NOT NULL,
  idDaughterProd INT NOT NULL,
  PRIMARY KEY (idRelation),
  INDEX Daughter (idDaughterProd ASC),
  INDEX Mother (idMotherProd ASC),
  CONSTRAIN `mother_daugher`
    FOREIGN KEY (idMotherProd)
    REFERENCES Productions (idProduction)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAIN `daugher_mother`
    FOREIGN KEY (idDaughterProd)
    REFERENCES Productions (idProduction)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = MyISAM;
  

